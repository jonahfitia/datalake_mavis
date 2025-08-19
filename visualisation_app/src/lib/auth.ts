// src/lib/auth.ts
import NextAuth, { NextAuthOptions } from "next-auth"
import CredentialsProvider from "next-auth/providers/credentials"
// ou GoogleProvider, GitHubProvider etc.

export const authOptions: NextAuthOptions = {
  providers: [
    CredentialsProvider({
      name: "Credentials",
      credentials: {
        username: { label: "Username", type: "text" },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        // 👉 ta logique de login
        if (
          credentials?.username === "jonah" &&
          credentials?.password === "1234"
        ) {
          return { id: "1", name: "Jonah", email: "jonah@example.com" }
        }
        return null
      },
    }),
  ],
  pages: {
    signIn: "/login",
  },
  session: {
    strategy: "jwt",
  },
  secret: process.env.NEXTAUTH_SECRET,
}

// Ici on exporte aussi le handler (utile pour l'API route)
const handler = NextAuth(authOptions)
export { handler as GET, handler as POST }
