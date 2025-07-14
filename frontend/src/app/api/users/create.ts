// pages/api/user/create.ts
import { prisma } from "@/lib/prisma";
import { NextApiRequest, NextApiResponse } from "next";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== "POST") return res.status(405).end();

    const { name, email, password, role } = req.body;

    try {
        const user = await prisma.user.create({
            data: {
                name,
                email,
                password,
                role, // "ADMIN" ou "DOCTOR"
            },
        });
        res.status(200).json(user);
    } catch (err) {
        res.status(500).json({ error: "Erreur lors de la création de l'utilisateur" });
    }
}
