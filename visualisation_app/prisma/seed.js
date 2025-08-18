const { PrismaClient } = require("@prisma/client");
const bcrypt = require("bcryptjs");

const prisma = new PrismaClient();

async function main() {
  const hashedPassword = await bcrypt.hash("jonah", 10);

  await prisma.user.upsert({
    where: { email: "jonahrafit@gmail.com" },
    update: {},
    create: {
      firstName: "JONAH",
      lastName: "FITIA",
      email: "jonahrafit@gmail.com",
      password: hashedPassword,
      role: "ADMIN", // string au lieu de enum TypeScript
    },
  });

  console.log("✅ Admin created");
}

main()
  .catch(console.error)
  .finally(() => prisma.$disconnect());
