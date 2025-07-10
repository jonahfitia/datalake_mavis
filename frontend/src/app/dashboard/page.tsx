// app/dashboard/page.tsx
import DashboardClient from "./DashboardClient"; 
import { getServerSession } from "next-auth";
import { authOptions } from "@/app/api/auth/[...nextauth]/route";
import { redirect } from "next/navigation";

// ✅ Déclare ici l’interface des props attendues
interface DashboardClientProps {
  userName: string;
}

export default async function DashboardPage() {
  const session = await getServerSession(authOptions);

  if (!session) {
    redirect("/login");
  }

  // Tu peux aussi extraire des infos comme le nom de l'utilisateur ici
  const userName = session.user?.name || "Utilisateur";

  return <DashboardClient userName={userName} />;
}
