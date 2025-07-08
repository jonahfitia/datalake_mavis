"use client";

import { useEffect, useRef } from "react";
import * as d3 from "d3";

export default function ChartLine() {
  const ref = useRef<SVGSVGElement | null>(null);

  useEffect(() => {
    if (!ref.current) return;

    const data = [
      { x: 0, y: 30 },
      { x: 1, y: 80 },
      { x: 2, y: 45 },
      { x: 3, y: 60 },
      { x: 4, y: 20 },
      { x: 5, y: 90 },
      { x: 6, y: 50 },
    ];

    const width = 300;
    const height = 150;

    const svg = d3.select(ref.current);
    svg.selectAll("*").remove();
    svg.attr("width", width).attr("height", height);

    const x = d3.scaleLinear().domain([0, 6]).range([0, width]);
    const y = d3.scaleLinear().domain([0, 100]).range([height, 0]);

    const line = d3
      .line<{ x: number; y: number }>()
      .x((d) => x(d.x))
      .y((d) => y(d.y));

    svg
      .append("path")
      .datum(data)
      .attr("fill", "none")
      .attr("stroke", "orange")
      .attr("stroke-width", 2)
      .attr("d", line(data) ?? ""); // ✅ appelée ici
  }, []);

  return (
    <div>
      <h2 className="font-semibold mb-2">Line Chart</h2>
      <svg ref={ref}></svg>
    </div>
  );
}
