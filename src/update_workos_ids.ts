/**
 * WorkOS ID SQL Generator Tool
 * 
 * Process:
 * 1. Read mapping CSV file (workos_user_id,auth0_user_id,created)
 * 2. Read users_to_keep.jsonl file 
 * 3. For each user to keep, find matching auth0_user_id in mapping
 * 4. Generate SQL UPDATE statement to set workOSUserId in database
 * 5. Write all SQL statements to output file
 */

import dotenv from "dotenv";
import fs from "fs/promises";
import Papa from "papaparse";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";

dotenv.config();

interface WorkOSMapping {
    workos_user_id: string;
    auth0_user_id: string;
    created: string;
}

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

/**
 * Read and parse the WorkOS mapping CSV file
 */
async function loadWorkOSMappings(csvFile: string): Promise<Map<string, string>> {
    console.log(`Reading WorkOS mappings from ${csvFile}`);
    
    const csvContent = await fs.readFile(csvFile, 'utf-8');
    const parsed = Papa.parse<WorkOSMapping>(csvContent, {
        header: true,
        skipEmptyLines: 'greedy',
    });

    if (parsed.errors.length > 0) {
        console.error('CSV parsing errors:', parsed.errors);
        throw new Error('Failed to parse WorkOS mapping CSV');
    }

    // Create map: auth0_user_id -> workos_user_id
    const mappings = new Map<string, string>();
    parsed.data.forEach(row => {
        if (row.auth0_user_id && row.workos_user_id) {
            mappings.set(row.auth0_user_id, row.workos_user_id);
        }
    });

    console.log(`Loaded ${mappings.size} WorkOS mappings`);
    return mappings;
}

/**
 * Read users_to_keep.jsonl file line by line
 */
async function* readUsersToKeep(filePath: string): AsyncGenerator<ProcessingResult> {
    const fileContent = await fs.readFile(filePath, 'utf-8');
    const lines = fileContent.trim().split('\n');
    
    for (const line of lines) {
        if (line.trim()) {
            try {
                yield JSON.parse(line) as ProcessingResult;
            } catch (error) {
                console.error('Failed to parse line:', line, error);
            }
        }
    }
}

/**
 * Generate SQL UPDATE statement for a user
 */
function generateSQLUpdate(
    result: ProcessingResult, 
    workOSMappings: Map<string, string>
): string | null {
    if (!result.userToKeep || !result.auth0User) {
        return null;
    }

    const workOSUserId = workOSMappings.get(result.auth0User.user_id);
    if (!workOSUserId) {
        console.warn(`No WorkOS mapping found for ${result.email}: ${result.auth0User.user_id}`);
        return null;
    }

    // Generate SQL UPDATE statement with both id and email for safety
    // Parse ID as number (handles "57,027" -> 57027)
    const numericId = parseInt(result.userToKeep.id.replace(/,/g, ''), 10);
    const idClause = !isNaN(numericId) ? `id = ${numericId}` : `id = '${result.userToKeep.id}'`;
    
    const sql = `UPDATE users SET "workOSUserId" = '${workOSUserId}' WHERE ${idClause} AND email = '${result.userToKeep.email}';`;
    
    console.log(`Generated SQL for ${result.email}: ${result.userToKeep.id} -> ${workOSUserId}`);
    return sql;
}

/**
 * Main processing function
 */
async function main() {
    const {
        mappingFile,
        usersFile,
        outputFile,
        dryRun,
    } = await yargs(hideBin(process.argv))
        .option("mapping-file", {
            type: "string",
            default: "workos_mapping.csv",
            description: "Path to CSV file with WorkOS mappings (workos_user_id,auth0_user_id,created)",
        })
        .option("users-file", {
            type: "string", 
            default: "out/users_to_keep.jsonl",
            description: "Path to users_to_keep.jsonl file to update",
        })
        .option("output-file", {
            type: "string",
            default: "out/update_workos_ids.sql",
            description: "Path to output SQL UPDATE statements",
        })
        .option("dry-run", {
            type: "boolean",
            default: false,
            description: "Don't write output file, just log results",
        })
        .version(false)
        .parse();

    // Load WorkOS mappings
    const workOSMappings = await loadWorkOSMappings(mappingFile);

    // Process users_to_keep.jsonl and generate SQL statements
    const sqlStatements: string[] = [];
    let processedCount = 0;
    let sqlGeneratedCount = 0;

    console.log(`\nProcessing users from ${usersFile}`);

    // Add SQL header comment
    sqlStatements.push('-- SQL UPDATE statements to set workOSUserId for duplicate users');
    sqlStatements.push('-- Generated by update-workos-ids tool');
    sqlStatements.push(`-- Generated on: ${new Date().toISOString()}`);
    sqlStatements.push('');

    for await (const result of readUsersToKeep(usersFile)) {
        processedCount++;
        
        if (result.action === 'keep' && result.userToKeep && result.auth0User) {
            const sqlStatement = generateSQLUpdate(result, workOSMappings);
            
            if (sqlStatement) {
                sqlStatements.push(sqlStatement);
                sqlGeneratedCount++;
            }
        }
    }

    // Write SQL statements to file
    if (!dryRun) {
        const outputDir = outputFile.substring(0, outputFile.lastIndexOf('/'));
        await fs.mkdir(outputDir, { recursive: true });

        const sqlContent = sqlStatements.join('\n');
        await fs.writeFile(outputFile, sqlContent);
    }

    // Summary
    console.log('\n=== SQL GENERATION SUMMARY ===');
    console.log(`Total results processed: ${processedCount}`);
    console.log(`SQL UPDATE statements generated: ${sqlGeneratedCount}`);
    console.log(`Users without WorkOS mapping: ${processedCount - sqlGeneratedCount}`);
    
    if (!dryRun) {
        console.log(`\nSQL file written to: ${outputFile}`);
        console.log(`Execute the SQL statements to update the database.`);
    }
}

export default function start() {
    main().catch((err) => {
        console.error('Script failed:', err);
        process.exit(1);
    });
}