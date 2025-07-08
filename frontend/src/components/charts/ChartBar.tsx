"use client";

import { useEffect, useRef } from "react";
import * as d3 from "d3";

export default function ChartBar() {
  const ref = useRef(null);

  useEffect(() => {
    if (!ref.current) return;
    const data = [30, 80, 45, 60, 20, 90, 50];

    const svg = d3.select(ref.current);
    svg.selectAll("*").remove();

    const width = 300;
    const height = 150;
    svg.attr("width", width).attr("height", height);

    const x = d3
      .scaleBand()
      .domain(data.map((d, i) => i.toString()))
      .range([0, width])
      .padding(0.1);

    const y = d3.scaleLinear().domain([0, 100]).range([height, 0]);

    svg
      .selectAll("rect")
      .data(data)
      .join("rect")
      .attr("x", (_, i) => x(i.toString()) ?? 0)
      .attr("y", (d) => y(d))
      .attr("width", x.bandwidth())
      .attr("height", (d) => height - y(d))
      .attr("fill", "steelblue");
  }, []);

  return (
    <div>
      <h2 className="font-semibold mb-2">Bar Chart</h2>
      <svg ref={ref}></svg>
    </div>
  );
}
