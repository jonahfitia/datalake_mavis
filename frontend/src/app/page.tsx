import Heatmap from '@/components/Heatmap'

const sampleData = [
  { x: 'Lun', y: 'Matin', value: 10 },
  { x: 'Lun', y: 'Après-midi', value: 20 },
  { x: 'Mar', y: 'Matin', value: 5 },
  { x: 'Mar', y: 'Après-midi', value: 25 },
  { x: 'Mer', y: 'Matin', value: 15 },
  { x: 'Mer', y: 'Après-midi', value: 30 },
]

export default function Home() {
  return (
    <div className="grid grid-rows-[20px_1fr_20px] items-center justify-items-center min-h-screen p-8 pb-20 gap-16 sm:p-20 font-[family-name:var(--font-geist-sans)]">
      <main className="flex flex-col gap-[32px] row-start-2 items-center sm:items-start">
        <Heatmap width={300} height={200} data={sampleData} />
      </main>
    </div>
  );
}
