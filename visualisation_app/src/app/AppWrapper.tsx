"use client";

import { usePathname } from "next/navigation";
import { ToastContainer } from "react-toastify";
import Header from "@/components/Header";

export default function AppWrapper({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();

  return (
    <>
      {pathname !== "/login" && <Header />}
      {children}
      <ToastContainer />
    </>
  );
}
