import { useEffect, useState } from "react";

interface User {
  id: number;
  name: string;
}

export default function UserList() {
  const [users, setUsers] = useState<User[]>([]); // ✅ Typage ici

  useEffect(() => {
    // TODO: remplacer par fetch API réel
    setUsers([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ]);
  }, []);

  return (
    <div className="p-4">
      <h1 className="text-xl font-semibold mb-4">Liste des utilisateurs</h1>
      <ul className="list-disc pl-5">
        {users.map((user) => (
          <li key={user.id}>
            {user.name} - <a href={`/users/${user.id}/edit`} className="text-blue-600 underline">Modifier</a>
          </li>
        ))}
      </ul>
    </div>
  );
}
