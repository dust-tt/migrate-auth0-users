import { z } from "zod";

/**
 * These fields match the defaults from Auth0's "User Import / Export" extension:
 *
 *   https://auth0.com/docs/customize/extensions/user-import-export-extension
 */
export const Auth0ExportedUser = z.object({
  user_id: z.string(),
  email: z.string(),
  email_verified: z.any().optional(),
  name: z.string(),
  family_name: z.string().optional(),
  given_name: z.string().optional(),
  nickname: z.string(),
  picture: z.string(),
  provider: z.string(),
  created_at: z.string(),
  updated_at: z.string(),
  region: z.string().optional(),
  workos_user_id: z.string().optional(),
});

export type Auth0ExportedUser = z.infer<typeof Auth0ExportedUser>;
