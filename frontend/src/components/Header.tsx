"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { LogOut } from "lucide-react";
import { signOut } from "next-auth/react";

export default function Header() {
  return (
    <header className="bg-white shadow-md p-4 flex items-center justify-between">
      <div className="text-xl font-semibold text-blue-700">
        <Link href="/dashboard">Datalake MAVIS</Link>
      </div>

      <nav className="space-x-4">
        <Link href="/dashboard" className="text-gray-700 hover:text-blue-600">
          Dashboard
        </Link>
        <Link href="/about" className="text-gray-700 hover:text-blue-600">
          Mon compte
        </Link>
        <Link href="/about" className="text-gray-700 hover:text-blue-600">
          À propos
        </Link>
      </nav>

      <button
         onClick={() => signOut({ callbackUrl: "/login" })}
        className="flex items-center space-x-1 text-red-600 hover:text-red-800"
      >
        <LogOut size={18} />
        <span>Déconnexion</span>
      </button>
    </header>
  );
}
