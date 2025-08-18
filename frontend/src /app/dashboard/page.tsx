// app/dashboard/page.tsx
import DashboardClient from "./DashboardClient";
import { getServerSession } from "next-auth";
import { authOptions } from "@/app/api/auth/[...nextauth]/route";
import { redirect } from "next/navigation";

export default async function DashboardPage() {
  const session = await getServerSession(authOptions);

  if (!session) {
    // Rediriger vers la page login si pas connecté
    redirect("/auth/login");
    // redirect("/dashboard");
  }

  return <DashboardClient userName={session.user.email ?? "Utilisateur"} />;
}
