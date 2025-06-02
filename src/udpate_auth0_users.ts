import { ManagementApiError, ManagementClient } from "auth0";
import dotenv from "dotenv";
import Queue from "p-queue";
import { ndjsonStream } from "./ndjson-stream";
import { sleep } from "./sleep";
dotenv.config();

const auth0 = new ManagementClient({
  domain: process.env.AUTH0_TENANT_DOMAIN_URL!,
  token: process.env.AUTH0_API_TOKEN!,
});

interface MigrationResult {
  auth0_user_id: string;
  workos_user_id: string;
  created: boolean;
}

const DEFAULT_RETRY_AFTER = 10;
const MAX_CONCURRENT_USER_IMPORTS = 10;

async function processLine(
  line: unknown,
  recordNumber: number
): Promise<boolean> {
  const { auth0_user_id, workos_user_id, created } = line as MigrationResult;

  try {
    // Update the user's metadata with the WorkOS user ID
    await auth0.users.update(
      { id: auth0_user_id },
      {
        app_metadata: {
          workos_user_id,
        },
      }
    );

    console.log(
      `(${recordNumber}) Successfully updated user ${auth0_user_id} with WorkOS ID ${workos_user_id}`
    );
  } catch (error) {
    if (error instanceof ManagementApiError && error.statusCode === 429) {
      throw error;
    }
    console.error(
      `(${recordNumber}) Failed to update user ${auth0_user_id}:`,
      error
    );
    return false;
  }

  return true;
}

async function main() {
  console.log("Adding workos user id to Auth0 users");

  const queue = new Queue({ concurrency: MAX_CONCURRENT_USER_IMPORTS });

  let recordCount = 0;
  let completedCount = 0;

  for await (const line of ndjsonStream("./out/migration-results.jsonl")) {
    await queue.onSizeLessThan(MAX_CONCURRENT_USER_IMPORTS);

    const recordNumber = recordCount;
    const enqueueTask = () =>
      queue
        .add(async () => {
          const successful = await processLine(
            line,
            recordNumber
            // passwordStore
          );
          if (successful) {
            completedCount++;
          }
        })
        .catch(async (error: unknown) => {
          if (
            !(error instanceof ManagementApiError) ||
            error.statusCode !== 429
          ) {
            throw error;
          }

          const retryAfter =
            (Number(error.headers.get("retry-after")) ?? DEFAULT_RETRY_AFTER) +
            1;

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
