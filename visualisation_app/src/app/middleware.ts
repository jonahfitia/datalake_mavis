// middleware.ts
export { auth as middleware } from "@/lib/auth"

export const config = {
  matcher: [],
  // matcher: ["/dashboard/:path*", "/profile/:path*"],
  // matcher: ["/((?!login).*)"], // protège tout sauf /login
}
