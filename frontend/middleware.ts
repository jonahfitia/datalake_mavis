import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';

const allowedOrigins = [
  'http://localhost:3000',
  'http://192.168.2.139:3000',
];

const protectedPaths = [
  "/dashboard",
  "/profile",
];

const publicPaths = [
  "/auth/login",
  "/auth/signup",
];

// Utilitaire : vérifier si session existe via cookie
function isAuthenticated(request: NextRequest) {
  const sessionCookie = request.cookies.get('next-auth.session-token')?.value ||
                        request.cookies.get('__Secure-next-auth.session-token')?.value;
  return !!sessionCookie;
}

export function middleware(request: NextRequest) {
  const origin = request.headers.get('origin') || '';
  const url = request.nextUrl.clone();

  const response = NextResponse.next();

  // CORS Headers
  if (allowedOrigins.includes(origin)) {
    response.headers.set('Access-Control-Allow-Origin', origin);
    response.headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    response.headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  }

  const pathname = url.pathname;
  const authenticated = isAuthenticated(request);

  // 🔒 Rediriger vers /auth/login si la route est protégée et pas connecté
  if (protectedPaths.some(path => pathname.startsWith(path)) && !authenticated) {
    url.pathname = '/auth/login';
    return NextResponse.redirect(url);
  }

  // 🚫 Si connecté et route publique (login/signup), rediriger vers /dashboard
  if (publicPaths.some(path => pathname.startsWith(path)) && authenticated) {
    url.pathname = '/dashboard';
    return NextResponse.redirect(url);
  }

  return response;
}

export const config = {
  matcher: [
    '/dashboard/:path*',
    '/profile/:path*',
    '/auth/:path*',
    '/',
  ],
};
