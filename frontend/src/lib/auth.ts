import { PrismaAdapter } from "@next-auth/prisma-adapter";
import GoogleProvider from "next-auth/providers/google";
import type { NextAuthOptions } from "next-auth";
import { prisma } from "@/lib/prisma";

declare module "next-auth" {
  interface Session {
    user: {
      id: string;
      role: "ADMIN" | "DOCTOR";
      name?: string | null;
      email?: string | null;
      image?: string | null;
    };
  }

  interface User {
    id: string;
    role: "ADMIN" | "DOCTOR";
  }
}

export const authOptions: NextAuthOptions = {
  adapter: PrismaAdapter(prisma),

  providers: [
    GoogleProvider({
      clientId: process.env.GOOGLE_CLIENT_ID!,
      clientSecret: process.env.GOOGLE_CLIENT_SECRET!,
    }),
    // tu peux ajouter GitHub, Facebook, etc. ici
  ],

  session: {
    strategy: "database", // ✅ important
    maxAge: 30 * 24 * 60 * 60, // facultatif : 30 jours
  },
  callbacks: {
    async session({ session, user }) {
      if (!session.user?.email) return session;

      // On récupère le user dans la base si user est undefined (cas fréquent)
      const dbUser = user ?? await prisma.user.findUnique({
        where: { email: session.user.email },
      });

      if (dbUser && session.user) {
        session.user.id = dbUser.id;
        session.user.role = dbUser.role as "ADMIN" | "DOCTOR";
      }

      return session;
    },
  },

  secret: process.env.NEXTAUTH_SECRET,
  pages: {
    signIn: "/auth/login", // ta page personnalisée de login
  },
};
