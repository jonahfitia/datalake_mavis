"use client";

import { useParams, useRouter } from "next/navigation";
import { useEffect, useState } from "react";

export default function EditUser() {
  const params = useParams<{ id: string }>();
  const id = params?.id ?? "";

  const router = useRouter();
  const [name, setName] = useState("");

  useEffect(() => {
    // TODO: récupérer utilisateur via API avec id
    setName("Nom d'exemple");
  }, [id]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    // TODO: envoyer update vers API
    alert(`Utilisateur ${id} mis à jour avec le nom: ${name}`);
    router.push("/users");
  };

  return (
    <div className="p-4">
      <h1 className="text-xl font-semibold mb-4">Modifier utilisateur {id}</h1>
      <form onSubmit={handleSubmit} className="space-y-4 max-w-sm">
        <input
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
          className="border p-2 w-full"
        />
        <button type="submit" className="bg-blue-600 text-white px-4 py-2 rounded">
          Enregistrer
        </button>
      </form>
    </div>
  );
}
