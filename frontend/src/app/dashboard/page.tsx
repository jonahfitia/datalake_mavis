import ChartBar from "@/components/charts/ChartBar";

import ChartLine from "@/components/charts/ChartLine";
import ChartPie from "@/components/charts/ChartPie";
import ChartRadar from "@/components/charts/ChartRadar";
import LiveLineChart from "@/components/charts/LiveLineChart";

export default function Dashboard() {
  return (
    <div className="p-4 space-y-8">
      <h1 className="text-2xl font-bold mb-4">Tableau de visualisation</h1>
      <div className="grid grid-cols-2 gap-6">
        <ChartBar />
        <ChartLine />
        <ChartPie />
        <ChartRadar />
        <LiveLineChart />
      </div>
    </div>
  );
}
