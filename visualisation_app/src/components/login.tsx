'use client';

import { signIn } from "next-auth/react";
import { useState } from "react";
import { useRouter } from "next/navigation";

export default function Login() {
  const router = useRouter();
  const [email, setEmail] = useState("jonahrafit@gmail.com");
  const [password, setPassword] = useState("jonah");
  const [loading, setLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setErrorMessage("");

    const res = await signIn("credentials", {
      redirect: false,
      email,
      password,
    });

    console.log("signIn response:", res); // For debugging

    setLoading(false);

    if (res?.error) {
      setErrorMessage("Échec de connexion : email ou mot de passe invalide.");
      console.error("Erreur NextAuth:", res.error);
    } else {
      router.push("/dashboard");
    }
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-50 p-4">
      <form onSubmit={handleSubmit} className="w-full max-w-sm space-y-4 bg-white p-6 rounded shadow">
        <h2 className="text-xl font-semibold text-center">Connexion</h2>

        <input
          type="email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          placeholder="Adresse email"
          required
          className="w-full p-2 border rounded"
        />

        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Mot de passe"
          required
          className="w-full p-2 border rounded"
        />

        {errorMessage && (
          <p className="text-red-600 text-sm text-center">{errorMessage}</p>
        )}

        <button
          type="submit"
          className="w-full bg-blue-600 text-white p-2 rounded hover:bg-blue-700 transition"
          disabled={loading}
        >
          {loading ? "Connexion..." : "Se connecter"}
        </button>
      </form>
    </div>
  );
}
