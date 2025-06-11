import { RateLimitExceededException, WorkOS } from "@workos-inc/node";
import dotenv from "dotenv";
import Queue from "p-queue";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";

import { ManagementClient } from "auth0";
import { z } from "zod";
import { ndjsonStream } from "./ndjson-stream";
import { sleep } from "./sleep";

dotenv.config();

export const Auth0ExportedUser = z.object({
  _id: z.string(),
  email: z.string(),
  email_verified: z.any().optional(),
  tenant: z.string(),
  connection: z.string(),
  passwordHash: z.string(),
  _tmp_is_unique: z.any().optional(),
  version: z.string(),
  identifiers: z.array(
    z.object({
      type: z.string(),
      value: z.string(),
      verified: z.boolean(),
    })
  ),
});

export type Auth0ExportedUser = z.infer<typeof Auth0ExportedUser>;

const USE_LOCAL_API = (process.env.NODE_ENV ?? "").startsWith("dev");

const workos = new WorkOS(
  process.env.WORKOS_SECRET_KEY,
  USE_LOCAL_API
    ? {
        https: false,
        apiHostname: "localhost",
        port: 7000,
      }
    : {}
);

const auth0 = new ManagementClient({
  domain: process.env.AUTH0_TENANT_DOMAIN_URL!,
  token: process.env.AUTH0_API_TOKEN!,
});

async function updateUserPassword(exportedUser: Auth0ExportedUser) {
  try {
    const userId = `auth0|${exportedUser._id}`;
    const matchingUsers = await workos.userManagement.listUsers({
      email: exportedUser.email.toLowerCase(),
    });
    if (matchingUsers.data.length === 1) {
      const workOsUserId = matchingUsers.data[0].id;
      const workOsUser = await workos.userManagement.updateUser({
        userId: workOsUserId,
        passwordHash: exportedUser.passwordHash,
        passwordHashType: "bcrypt" as const,
      });

      try {
        await auth0.users.update(
          { id: userId },
          {
            app_metadata: {
              password_imported_to_workos: new Date().toISOString(),
            },
          }
        );
      } catch (error) {
        console.error(
          `Can't update metadata for user ${userId} with error ${exportedUser.email}`
        );
      }
      return workOsUser;
    } else {
      return undefined;
    }
  } catch (error) {
    if (error instanceof RateLimitExceededException) {
      throw error;
    }
  }
}

async function processLine(
  line: unknown,
  recordNumber: number
): Promise<boolean> {
  const exportedUser = Auth0ExportedUser.parse(line);

  const workOsUser = await updateUserPassword(exportedUser);
  if (!workOsUser) {
    console.error(
      `(${recordNumber}) Could not find or create user ${exportedUser._id} for ${exportedUser.email}`
    );
    return false;
  }

  console.log(
    `(${recordNumber}) Imported Auth0 password user ${exportedUser._id} for WorkOS user ${workOsUser.id}`
  );

  return true;
}

const DEFAULT_RETRY_AFTER = 10;
const MAX_CONCURRENT_USER_IMPORTS = 10;

async function main() {
  const { passwordExport: passwordFilePath, cleanupTempDb } = await yargs(
    hideBin(process.argv)
  )
    .option("password-export", {
      type: "string",
      required: true,
      description: "Path to the password export received from Auth0 support.",
      default: "./export_dust.jsonline",
    })
    .option("cleanup-temp-db", {
      type: "boolean",
      default: true,
      description:
        "Whether to delete the temporary sqlite database after finishing the migration.",
    })
    .version(false)
    .parse();

  console.log(`Importing password hashes from ${passwordFilePath}`);

  const queue = new Queue({ concurrency: MAX_CONCURRENT_USER_IMPORTS });

  let recordCount = 0;
  let completedCount = 0;

  for await (const line of ndjsonStream(passwordFilePath)) {
    await queue.onSizeLessThan(MAX_CONCURRENT_USER_IMPORTS);

    const recordNumber = recordCount;
    const enqueueTask = () =>
      queue
        .add(async () => {
          const successful = await processLine(line, recordNumber);
          if (successful) {
            completedCount++;
          }
        })
        .catch(async (error: unknown) => {
          if (!(error instanceof RateLimitExceededException)) {
            throw error;
          }

          const retryAfter = (error.retryAfter ?? DEFAULT_RETRY_AFTER) + 1;
          console.warn(
            `Rate limit exceeded. Pausing queue for ${retryAfter} seconds.`
          );

          queue.pause();
          enqueueTask();

          await sleep(retryAfter * 1000);

          queue.start();
        });
    enqueueTask();

    recordCount++;
  }

  await queue.onIdle();

  console.log(
    `Done importing. ${completedCount} of ${recordCount} user records imported.`
  );
}

export default function start() {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
