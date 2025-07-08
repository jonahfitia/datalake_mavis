"use client";

import { useState } from "react";
import ChartBar from "@/components/charts/TimeSeriesChart";
import ChartLine from "@/components/charts/ChartLine";
import ChartPie from "@/components/charts/ChartPie";
import ChartRadar from "@/components/charts/ChartRadar";
import LiveLineChart from "@/components/charts/LiveLineChart";
import { getServerSession } from "next-auth";
import { authOptions } from "@/app/api/auth/[...nextauth]/route";
import { redirect } from "next/navigation";
import TimeSeriesChart from "@/components/charts/TimeSeriesChart";

export default async function Dashboard() {
  const [selectedYear, setSelectedYear] = useState("2024");

  const years = ["2022", "2023", "2024", "2025"];
  const session = await getServerSession(authOptions);

  if (!session) {
    redirect("/login");
  }

  return (
    <div className="p-4 space-y-8">
      <h1 className="text-2xl font-bold mb-4">Tableau de visualisation</h1>

      <div className="grid grid-cols-12 gap-6">
        {/* Sidebar filtre - 3/12 */}
        <div className="col-span-12 md:col-span-3 bg-white p-4 rounded-xl shadow space-y-4">
          <h2 className="text-lg font-semibold">Filtres</h2>
          <div>
            <label htmlFor="year" className="block text-sm font-medium mb-1">
              Année
            </label>
            <select
              id="year"
              value={selectedYear}
              onChange={(e) => setSelectedYear(e.target.value)}
              className="w-full p-2 border border-gray-300 rounded-md"
            >
              {years.map((year) => (
                <option key={year} value={year}>
                  {year}
                </option>
              ))}
            </select>
          </div>
          {/* Tu peux ajouter d’autres filtres ici */}
        </div>

        {/* Zone de graphiques - 9/12 */}
        <div className="col-span-12 md:col-span-9 grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="bg-white p-4 rounded-xl shadow text-center">
            <ChartLine />
          </div>
          <div className="bg-white p-4 rounded-xl shadow text-center">
            <ChartPie />
          </div>
          <div className="bg-white p-4 rounded-xl shadow text-center">
            <ChartRadar />
          </div>
          <div className="bg-white p-4 rounded-xl shadow text-center">
            <LiveLineChart />
          </div>
          <TimeSeriesChart />
        </div>

      </div>
    </div>
  );
}
