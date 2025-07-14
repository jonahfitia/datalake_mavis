// prisma/seed.ts
import { PrismaClient } from "@prisma/client";
import { hash } from "bcryptjs";

const prisma = new PrismaClient();

async function main() {
  const hashedPassword = await hash("jonah", 10);

  const admin = await prisma.user.upsert({
    where: { email: "jonahrafit@gmail.com" },
    update: {},
    create: {
      name: "JONAH",
      email: "jonahrafit@gmail.com",
      password: hashedPassword,
      role: "ADMIN",
    },
  });

  console.log("✅ Admin created:", admin);
}

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error("❌ Error during seeding:", e);
    await prisma.$disconnect();
    process.exit(1);
  });
