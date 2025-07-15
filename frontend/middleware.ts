import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';
import { getToken } from "next-auth/jwt";

const allowedOrigins = [
    'http://localhost:3000',
    'http://192.168.2.139:3000',
];

// Pages protégées (tu peux adapter la liste)
const protectedPaths = [
    "/dashboard",
    "/profile",
    // ... ajoute ici d’autres chemins privés
];

// Pages publiques où on ne veut pas être redirigé si connecté (ex: login, signup)
const publicPaths = [
    "/auth/login",
    "/auth/signup",
];

export async function middleware(request: NextRequest) {
    const origin = request.headers.get('origin') || '';
    const url = request.nextUrl.clone();

    // CORS headers
    const response = NextResponse.next();
    if (allowedOrigins.includes(origin)) {
        response.headers.set('Access-Control-Allow-Origin', origin);
        response.headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
        response.headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    }

    // Vérifier le token NextAuth
    const token = await getToken({ req: request, secret: process.env.NEXTAUTH_SECRET });

    const pathname = url.pathname;

    // Si la route est protégée et pas de token => rediriger vers login
    if (protectedPaths.some(path => pathname.startsWith(path)) && !token) {
        url.pathname = "/auth/login";
        return NextResponse.redirect(url);
    }

    // Si la route est publique (login/signup) et utilisateur connecté => rediriger vers dashboard
    if (publicPaths.some(path => pathname.startsWith(path)) && token) {
        url.pathname = "/dashboard";
        return NextResponse.redirect(url);
    }

    return response;
}

export const config = {
    matcher: [
        "/dashboard/:path*",
        "/profile/:path*",
        "/auth/:path*",
        "/",
    ],
};
