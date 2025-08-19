"use client";

import { useEffect, useRef } from "react";
import * as d3 from "d3";

type Category = "success" | "failed" | "neutral";

type DataItem = {
  label: string;
  success: number;
  failed: number;
  neutral: number;
};

export default function ChartStackedBar() {
  const ref = useRef<SVGSVGElement | null>(null);

  useEffect(() => {
    if (!ref.current) return;

    const svg = d3.select(ref.current);
    svg.selectAll("*").remove(); // Clear

    const width = 400;
    const height = 200;
    const margin = { top: 20, right: 20, bottom: 30, left: 80 };

    svg.attr("width", width).attr("height", height);

    // ✅ Données typées
    const data: DataItem[] = [
      { label: "Group A", success: 30, failed: 20, neutral: 10 },
      { label: "Group B", success: 40, failed: 10, neutral: 20 },
      { label: "Group C", success: 20, failed: 30, neutral: 10 },
    ];

    const categories: Category[] = ["success", "failed", "neutral"];

    const colors = d3
      .scaleOrdinal<Category, string>()
      .domain(categories)
      .range(["#4ade80", "#f87171", "#facc15"]);

    // ✅ Stack layout typé
    const stack = d3.stack<DataItem, Category>().keys(categories);
    const stackedData = stack(data);

    const y = d3
      .scaleBand()
      .domain(data.map(d => d.label))
      .range([margin.top, height - margin.bottom])
      .padding(0.2);

    const x = d3
      .scaleLinear()
      .domain([
        0,
        d3.max(data, d => d.success + d.failed + d.neutral)!,
      ])
      .nice()
      .range([margin.left, width - margin.right]);

    // ✅ Dessiner les barres empilées (horizontales)
    svg
      .selectAll("g.layer")
      .data(stackedData)
      .enter()
      .append("g")
      .attr("fill", d => colors(d.key))
      .selectAll("rect")
      .data(d => d)
      .enter()
      .append("rect")
      .attr("x", d => x(d[0]))
      .attr("y", d => y((d.data as DataItem).label)!)
      .attr("width", d => x(d[1]) - x(d[0]))
      .attr("height", y.bandwidth());

    // ✅ Axe Y (Groupes)
    svg
      .append("g")
      .attr("transform", `translate(${margin.left},0)`)
      .call(d3.axisLeft(y).tickSize(0))
      .selectAll("text")
      .style("font-size", "12px");

    // ✅ Axe X (Valeurs empilées)
    svg
      .append("g")
      .attr("transform", `translate(0,${height - margin.bottom})`)
      .call(d3.axisBottom(x).ticks(5))
      .selectAll("text")
      .style("font-size", "12px");
  }, []);

  return (
    <div>
      <h2 className="font-semibold mb-2">Stacked Horizontal Bar Chart</h2>
      <svg ref={ref}></svg>
    </div>
  );
}
