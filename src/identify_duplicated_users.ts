/**
 * Duplicate User Resolution Tool
 * 
 * Process:
 * 1. Read CSV file containing duplicate users grouped by email
 * 2. For each email group, query Auth0 to find existing users
 * 3. Match duplicate users with Auth0 users using auth0Sub field
 * 4. Apply resolution logic:
 *    - If 0 matches: Skip (users deleted from Auth0) -> skipped_users.jsonl
 *    - If 1 match: Keep that user -> users_to_keep.jsonl  
 *    - If 2+ matches: Manual review required -> manual_review.jsonl
 * 
 * Output files contain the full context needed for next processing steps.
 */

import { ManagementClient } from "auth0";
import dotenv from "dotenv";
import fs from "fs/promises";
import Papa from "papaparse";
import Queue from "p-queue";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import { sleep } from "./sleep";

dotenv.config();

interface DuplicateUser {
    username: string;
    email: string;
    name: string;
    createdAt: string;
    updatedAt: string;
    provider?: string;
    providerId?: string;
    isDustSuperUser: boolean;
    firstName: string;
    lastName?: string;
    imageUrl?: string;
    auth0Sub?: string;
    sId: string;
    id: string;
    workOSUserId?: string;
}

interface Auth0User {
    user_id: string;
    email: string;
    last_login?: string;
    last_ip?: string;
    logins_count?: number;
    created_at: string;
    updated_at: string;
}

interface ProcessingResult {
    email: string;
    duplicates: DuplicateUser[];
    auth0Users: Auth0User[];
    userToKeep?: DuplicateUser;
    auth0User?: Auth0User;
    action: 'keep' | 'skip' | 'manual_review';
    reason: string;
    requiresManualReview?: boolean;
}

const auth0 = new ManagementClient({
    domain: process.env.AUTH0_TENANT_DOMAIN_URL!,
    clientId: process.env.AUTH0_M2M_CLIENT_ID!,
    clientSecret: process.env.AUTH0_M2M_CLIENT_SECRET!,
});

const MAX_CONCURRENT_REQUESTS = 5;

/**
 * Fetch Auth0 users by email address
 * Uses Lucene query syntax with proper character escaping
 */
async function getAuth0UsersByEmail(email: string): Promise<Auth0User[]> {
    try {
        const response = await auth0.users.getAll({
            q: `email:"${email.replace(/([+\-&|!(){}[\]^"~*?:\\\/])/g, "\\$1")}"`,
            fields: 'user_id,email,last_login,last_ip,logins_count,created_at,updated_at',
            include_fields: true,
        });
        return response.data as Auth0User[] || [];
    } catch (error) {
        console.error(`Failed to fetch Auth0 users for email ${email}:`, error);
        return [];
    }
}

/**
 * Determines which duplicate user to keep based on Auth0 validation
 * Process:
 * 1. Check which users have auth0Sub that matches actual Auth0 user_id
 * 2. If none match -> skip (user was deleted from Auth0)
 * 3. If one matches -> keep that user
 * 4. If multiple match -> manual review required
 */
function processDuplicateUsers(
    duplicates: DuplicateUser[],
    auth0Users: Auth0User[]
): {
    userToKeep?: DuplicateUser;
    auth0User?: Auth0User;
    action: 'keep' | 'skip' | 'manual_review';
    reason: string;
    requiresManualReview?: boolean;
} {
    // Create lookup map: auth0 user_id -> Auth0 user data
    const auth0UserMap = new Map<string, Auth0User>();
    auth0Users.forEach(user => {
        auth0UserMap.set(user.user_id, user);
    });

    // Find duplicate users that exist in Auth0 (auth0Sub matches actual user_id)
    const existingUsers = duplicates.filter(user => 
        user.auth0Sub && auth0UserMap.has(user.auth0Sub)
    );

    // Case 1: No users exist in Auth0 -> skip
    if (existingUsers.length === 0) {
        return {
            action: 'skip',
            reason: 'All users deleted from Auth0'
        };
    }

    // Case 2: Only one user exists in Auth0 -> keep that user
    if (existingUsers.length === 1) {
        const user = existingUsers[0];
        return {
            userToKeep: user,
            auth0User: auth0UserMap.get(user.auth0Sub!),
            action: 'keep',
            reason: 'Single user exists in Auth0'
        };
    }

    // Case 3: Multiple users exist in Auth0 -> manual review required
    // Sort by most recent activity to suggest the best candidate
    const sortedUsers = existingUsers
        .map(user => ({
            user,
            auth0Data: auth0UserMap.get(user.auth0Sub!)!,
        }))
        .sort((a, b) => {
            // Sort by login count first, then by last login
            const loginDiff = (b.auth0Data.logins_count || 0) - (a.auth0Data.logins_count || 0);
            if (loginDiff !== 0) return loginDiff;
            
            if (a.auth0Data.last_login && b.auth0Data.last_login) {
                return new Date(b.auth0Data.last_login).getTime() - new Date(a.auth0Data.last_login).getTime();
            }
            return 0;
        });

    return {
        userToKeep: sortedUsers[0].user,
        auth0User: sortedUsers[0].auth0Data,
        action: 'manual_review',
        reason: `${existingUsers.length} users exist in Auth0 - manual review required`,
        requiresManualReview: true
    };
}

/**
 * Process a group of duplicate users for a specific email
 * 1. Fetch Auth0 users for the email
 * 2. Determine which duplicate user to keep based on Auth0 validation
 */
async function processEmailGroup(
    email: string,
    duplicates: DuplicateUser[],
    recordNumber: number
): Promise<ProcessingResult> {
    console.log(`(${recordNumber}) Processing ${duplicates.length} duplicates for ${email}`);

    // Fetch current Auth0 users for this email
    const auth0Users = await getAuth0UsersByEmail(email);
    console.log(`(${recordNumber}) Found ${auth0Users.length} Auth0 users for ${email}`);

    // Determine what action to take with these duplicates
    const {
        userToKeep,
        auth0User,
        action,
        reason,
        requiresManualReview
    } = processDuplicateUsers(duplicates, auth0Users);

    return {
        email,
        duplicates,
        auth0Users,
        userToKeep,
        auth0User,
        action,
        reason,
        requiresManualReview
    };
}

/**
 * Main processing function
 * Orchestrates the entire duplicate user resolution workflow
 */
async function main() {
    const {
        csvFile,
        outputFile,
        manualReviewFile,
        skippedFile,
        rateLimitThreshold,
        dryRun,
    } = await yargs(hideBin(process.argv))
        .option("csv-file", {
            type: "string",
            default: "duplicated_emails.csv",
            description: "Path to the CSV file containing duplicate users",
        })
        .option("output-file", {
            type: "string",
            default: "out/users_to_keep.jsonl",
            description: "Path to output users that should be kept",
        })
        .option("manual-review-file", {
            type: "string",
            default: "out/manual_review.jsonl",
            description: "Path to output cases requiring manual review",
        })
        .option("skipped-file", {
            type: "string",
            default: "out/skipped_users.jsonl",
            description: "Path to output skipped (deleted) users",
        })
        .option("rate-limit-threshold", {
            type: "number",
            default: 3,
            description: "Rate limit threshold for Auth0 API calls",
        })
        .option("dry-run", {
            type: "boolean",
            default: false,
            description: "Don't write output file, just log results",
        })
        .version(false)
        .parse();

    console.log(`Reading duplicate users from ${csvFile}`);

    // Read and parse CSV
    const csvContent = await fs.readFile(csvFile, 'utf-8');
    const parsed = Papa.parse<DuplicateUser>(csvContent, {
        header: true,
        skipEmptyLines: 'greedy',
        transform: (value, field) => {
            // Convert string 'true'/'false' to boolean for isDustSuperUser
            if (field === 'isDustSuperUser') {
                return value.toLowerCase() === 'true';
            }
            return value;
        }
    });

    if (parsed.errors.length > 0) {
        console.error('CSV parsing errors:', parsed.errors);
        process.exit(1);
    }

    // Group users by email
    const usersByEmail = new Map<string, DuplicateUser[]>();
    parsed.data.forEach(user => {
        if (!usersByEmail.has(user.email)) {
            usersByEmail.set(user.email, []);
        }
        usersByEmail.get(user.email)!.push(user);
    });

    console.log(`Found ${usersByEmail.size} unique emails with duplicates`);

    const queue = new Queue({ concurrency: MAX_CONCURRENT_REQUESTS });
    const results: ProcessingResult[] = [];
    let recordCount = 0;

    // Ensure output directories exist
    if (!dryRun) {
        const dirs = new Set([
            outputFile.substring(0, outputFile.lastIndexOf('/')),
            manualReviewFile.substring(0, manualReviewFile.lastIndexOf('/')),
            skippedFile.substring(0, skippedFile.lastIndexOf('/'))
        ]);
        
        for (const dir of dirs) {
            await fs.mkdir(dir, { recursive: true });
        }
    }

    for (const [email, duplicates] of usersByEmail.entries()) {
        const recordNumber = recordCount++;

        await queue.add(async () => {
            try {
                const result = await processEmailGroup(email, duplicates, recordNumber);
                results.push(result);

                if (!dryRun) {
                    // Write result to appropriate output file based on action
                    let targetFile: string;
                    if (result.action === 'keep') {
                        targetFile = outputFile;
                    } else if (result.action === 'manual_review') {
                        targetFile = manualReviewFile;
                    } else { // skip
                        targetFile = skippedFile;
                    }
                    
                    await fs.writeFile(
                        targetFile,
                        JSON.stringify(result) + '\n',
                        { flag: 'a' }
                    );
                }

                // Log processing result
                console.log(`(${recordNumber}) ${email}: ${result.action} - ${result.reason}`);
                if (result.userToKeep) {
                    console.log(`(${recordNumber}) Selected user: ${result.userToKeep.sId} (${result.userToKeep.username})`);
                }
            } catch (error) {
                console.error(`(${recordNumber}) Error processing ${email}:`, error);
            }
        });

        // Simple rate limiting
        if (recordCount % rateLimitThreshold === 0) {
            await sleep(1000);
        }
    }

    await queue.onIdle();

    // Generate summary statistics
    const summary = {
        totalEmails: usersByEmail.size,
        actions: {
            keep: results.filter(r => r.action === 'keep').length,
            manual_review: results.filter(r => r.action === 'manual_review').length,
            skip: results.filter(r => r.action === 'skip').length,
        }
    };

    // Display summary
    console.log('\n=== PROCESSING SUMMARY ===');
    console.log(`Total duplicate email groups processed: ${summary.totalEmails}`);
    console.log(`Users to keep (single match): ${summary.actions.keep}`);
    console.log(`Manual review required (multiple matches): ${summary.actions.manual_review}`);
    console.log(`Skipped (deleted from Auth0): ${summary.actions.skip}`);

    if (!dryRun) {
        console.log('\n=== OUTPUT FILES ===');
        console.log(`Users to keep: ${outputFile}`);
        console.log(`Manual review cases: ${manualReviewFile}`);
        console.log(`Skipped users: ${skippedFile}`);
        
        // Write summary to file
        await fs.writeFile(
            outputFile.replace('.jsonl', '_summary.json'),
            JSON.stringify(summary, null, 2)
        );
    }
}

export default function start() {
    main().catch((err) => {
        console.error('Script failed:', err);
        process.exit(1);
    });
}