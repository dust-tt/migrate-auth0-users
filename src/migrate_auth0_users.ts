import { RateLimitExceededException, User, WorkOS } from "@workos-inc/node";
import dotenv from "dotenv";
import fs from "fs/promises";
import Queue from "p-queue";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";

import { Auth0ExportedUser } from "./auth0-exported-user";
import { ndjsonStream } from "./ndjson-stream";
// import { PasswordStore } from "./password-store";
import { sleep } from "./sleep";

dotenv.config();

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

async function updateUser(exportedUser: Auth0ExportedUser, existingUser: User) {
  await workos.userManagement.updateUser({
    userId: existingUser.id,
    emailVerified:
      existingUser.emailVerified ||
      exportedUser.email_verified === true ||
      exportedUser.email_verified === "true",
    firstName: exportedUser.given_name,
    lastName: exportedUser.family_name,
    metadata: {
      auth0Sub: exportedUser.user_id,
      ...(exportedUser.region ? { region: exportedUser.region } : {}),
    },
    // ...passwordOptions,
  });
  return { workOsUser: existingUser, created: false };
}

async function findOrCreateUser(
  exportedUser: Auth0ExportedUser
  // passwordHash: string | undefined
) {
  try {
    // const passwordOptions = passwordHash
    //   ? {
    //       passwordHash,
    //       passwordHashType: "bcrypt" as const,
    //     }
    //   : {};

    if (exportedUser.workos_user_id) {
      const user = await workos.userManagement.getUser(
        exportedUser.workos_user_id
      );
      return updateUser(exportedUser, user);
    }

    const user = await workos.userManagement.createUser({
      email: exportedUser.email,
      emailVerified:
        exportedUser.email_verified === true ||
        exportedUser.email_verified === "true",
      firstName: exportedUser.given_name,
      lastName: exportedUser.family_name,
      metadata: {
        auth0Sub: exportedUser.user_id,
        ...(exportedUser.region ? { region: exportedUser.region } : {}),
      },
      // ...passwordOptions,
    });
    return { workOsUser: user, created: true };
  } catch (error) {
    if (error instanceof RateLimitExceededException) {
      throw error;
    }

    const matchingUsers = await workos.userManagement.listUsers({
      email: exportedUser.email.toLowerCase(),
    });
    if (matchingUsers.data.length === 1) {
      const existingUser = matchingUsers.data[0];
      return updateUser(exportedUser, existingUser);
    } else {
      console.log("Can't migrate user", error);
      return { workOsUser: undefined, created: false };
    }
  }
}

async function processLine(
  line: unknown,
  recordNumber: number
  // passwordStore: PasswordStore
): Promise<boolean> {
  let exportedUser: Auth0ExportedUser;
  try {
    exportedUser = Auth0ExportedUser.parse(line);
  } catch (error) {
    console.error(`(${recordNumber}) Error parsing user: ${error}`);
    return false;
  }

  // const password = await passwordStore.find(exportedUser.user_id);
  // if (!password) {
  //   console.log(
  //     `(${recordNumber}) No password found in export for ${exportedUser.user_id}`
  //   );
  // }

  const { workOsUser, created } = await findOrCreateUser(
    exportedUser
    // password?.password_hash
  );
  if (!workOsUser) {
    console.error(
      `(${recordNumber}) Could not find or create user ${exportedUser.user_id}`
    );
    return false;
  }
  // Use writeFile with 'a' flag to ensure atomic append operation
  await fs.writeFile(
    "out/migration-results.jsonl",
    JSON.stringify({
      workos_user_id: workOsUser.id,
      auth0_user_id: exportedUser.user_id,
      created,
    }) + "\n",
    { flag: "a" }
  );

  if (created) {
    console.log(
      `(${recordNumber}) Imported Auth0 user ${exportedUser.user_id} as WorkOS user ${workOsUser.id}`
    );
  } else {
    console.log(
      `(${recordNumber}) Updated Auth0 user ${exportedUser.user_id} as WorkOS user ${workOsUser.id}`
    );
  }

  return true;
}

const DEFAULT_RETRY_AFTER = 60;
const MAX_CONCURRENT_USER_IMPORTS = 10;

async function main() {
  const {
    // passwordExport: passwordFilePath,
    userExport: userFilePath,
    skip,
    // cleanupTempDb,
  } = await yargs(hideBin(process.argv))
    .option("user-export", {
      type: "string",
      default: "out/users.jsonl",
      description:
        "Path to the user export created by the Auth0 export extension.",
    })
    .option("skip", {
      type: "number",
      default: 0,
      description: "Number of users to skip.",
    })
    // .option("password-export", {
    //   type: "string",
    //   description: "Path to the password export received from Auth0 support.",
    // })
    // .option("cleanup-temp-db", {
    //   type: "boolean",
    //   default: true,
    //   description:
    //     "Whether to delete the temporary sqlite database after finishing the migration.",
    // })
    .version(false)
    .parse();

  // console.log(`Importing password hashes from ${passwordFilePath}`);
  //
  // const passwordStore = await new PasswordStore().fromPasswordExport(
  //   passwordFilePath
  // );

  console.log(`Importing users from ${userFilePath}`);

  const queue = new Queue({ concurrency: MAX_CONCURRENT_USER_IMPORTS });

  let recordCount = 0;
  let completedCount = 0;

  try {
    for await (const line of ndjsonStream(userFilePath)) {
      await queue.onSizeLessThan(MAX_CONCURRENT_USER_IMPORTS);

      const recordNumber = recordCount;

      if (recordNumber >= skip) {
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
      }

      recordCount++;
    }

    await queue.onIdle();

    console.log(
      `Done importing. ${completedCount} of ${recordCount} user records imported.`
    );
  } finally {
    // passwordStore.destroy();
    // if (cleanupTempDb) {
    //   await fs.rm(passwordStore.dbPath);
    // }
  }
}

export default function start() {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
