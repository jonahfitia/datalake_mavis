import { defineConfig } from '@prisma/config'
import * as dotenv from 'dotenv'

// Charge les variables d'environnement depuis le .env
dotenv.config()

export default defineConfig({
  schema: './prisma/schema.prisma',
})
