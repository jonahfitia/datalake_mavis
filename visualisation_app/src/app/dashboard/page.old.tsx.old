// src/app/dashboard/page.tsx
"use client"

import { useSession, signIn, signOut } from "next-auth/react"

export default function Dashboard() {
  const { data: session } = useSession()

  if (!session) {
    return (
      <div>
        <p>Pas connecté</p>
        <button onClick={() => signIn()}>Se connecter</button>
      </div>
    )
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-50 p-4">
      <p>Connecté en tant que {session.user?.email}</p>
      <button onClick={() => signOut()}>Se déconnecter</button>
    </div>
  )
}