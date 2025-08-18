// next-auth.d.ts
import { DefaultSession, DefaultUser } from "next-auth";

declare module "next-auth" {
  interface User extends DefaultUser {
    id: string;
    firstName?: string | null;
    lastName?: string | null;
    email?: string | null;
  }

  interface Session {
    user: {
      id: string;
      firstName?: string | null;
      lastName?: string | null;
      email?: string | null;
    } & DefaultSession["user"];
  }
}

declare module "next-auth/jwt" {
  interface JWT {
    id: string;
    firstName?: string | null;
    lastName?: string | null;
    email?: string | null;
  }
}