"use client";

import { useEffect, useRef } from "react";
import * as d3 from "d3";

export default function ChartPie() {
  const ref = useRef<SVGSVGElement | null>(null);

  useEffect(() => {
    if (!ref.current) return;

    const data = [10, 20, 30, 40];

    const width = 200;
    const height = 200;
    const radius = Math.min(width, height) / 2;

    const svg = d3.select(ref.current);
    svg.selectAll("*").remove();
    svg.attr("width", width).attr("height", height);

    const g = svg
      .append("g")
      .attr("transform", `translate(${width / 2},${height / 2})`);

    const pie = d3.pie<number>();
    const arc = d3.arc<d3.PieArcDatum<number>>()
      .innerRadius(0)
      .outerRadius(radius);

    const color = d3.scaleOrdinal<number, string>(d3.schemeCategory10);

    const arcs = g.selectAll("g")
      .data(pie(data))
      .enter()
      .append("g");

    arcs
      .append("path")
      .attr("d", (d) => arc(d) ?? "") // ✅ sécurisé
      .attr("fill", (_, i) => color(i));
  }, []);

  return (
    <div>
      <h2 className="font-semibold mb-2">Pie Chart</h2>
      <svg ref={ref}></svg>
    </div>
  );
}
