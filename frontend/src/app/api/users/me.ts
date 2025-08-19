import { getServerSession } from "next-auth/next";
import { authOptions } from "@/lib/auth";
import { NextApiRequest, NextApiResponse } from "next";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    const session = await getServerSession(req, res, authOptions);

    if (!session) {
        return res.status(401).json({ message: "Non autorisé" });
    }

    res.status(200).json({
        message: "Voici vos données sécurisées",
        user: session.user,
    });
}
