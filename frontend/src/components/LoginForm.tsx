"use client";

import { useForm } from "react-hook-form";
import { signIn, SignInOptions } from "next-auth/react";
import { useRouter } from "next/navigation";
import { toast } from "react-toastify";

export default function LoginForm() {
  const { register, handleSubmit } = useForm();
  const router = useRouter();

  const onSubmit = async (data: SignInOptions | undefined) => {
    const res = await signIn("credentials", { redirect: false, ...data });
    if (res?.error) {
      toast.error("Échec de connexion");
    } else {
      router.push("/dashboard");
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100">
      <form
        onSubmit={handleSubmit(onSubmit)}
        className="bg-white p-8 rounded shadow-md w-80"
      >
        <h1 className="text-xl mb-4">Connexion</h1>
        <input
          className="border p-2 w-full mb-3"
          placeholder="Email"
          {...register("email")}
          type="email"
          required
        />
        <input
          className="border p-2 w-full mb-3"
          placeholder="Mot de passe"
          type="password"
          {...register("password")}
          required
        />
        <button
          type="submit"
          className="bg-blue-600 text-white py-2 px-4 rounded w-full"
        >
          Se connecter
        </button>
      </form>
    </div>
  );
}
