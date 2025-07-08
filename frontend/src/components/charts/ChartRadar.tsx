"use client";

import { useEffect, useRef } from "react";
import * as d3 from "d3";

export default function ChartRadar() {
  const ref = useRef<SVGSVGElement | null>(null);

  useEffect(() => {
    if (!ref.current) return;

    // Données radar simples
    const data = [
      { axis: "A", value: 0.8 },
      { axis: "B", value: 0.6 },
      { axis: "C", value: 0.9 },
      { axis: "D", value: 0.7 },
      { axis: "E", value: 0.5 },
    ];

    const width = 300;
    const height = 300;
    const radius = Math.min(width, height) / 2;

    const svg = d3.select(ref.current);
    svg.selectAll("*").remove();
    svg.attr("width", width).attr("height", height);

    const g = svg.append("g").attr("transform", `translate(${width / 2},${height / 2})`);

    const angleSlice = (Math.PI * 2) / data.length;
    const rScale = d3.scaleLinear().range([0, radius]).domain([0, 1]);

    // Correction ici : préciser le type dans lineRadial()
    const radarLine = d3.lineRadial<{ axis: string; value: number }>()
      .radius(d => rScale(d.value))
      .angle((d, i) => i * angleSlice)
      .curve(d3.curveLinearClosed);  // Optionnel : fermer la ligne

    g.append("path")
      .datum(data)
      .attr("d", radarLine as any)  // Ou caster en any pour éviter erreur TS si nécessaire
      .attr("fill", "lightblue")
      .attr("stroke", "steelblue")
      .attr("stroke-width", 2)
      .attr("fill-opacity", 0.5);

    // Draw axes
    data.forEach((d, i) => {
      const angle = i * angleSlice - Math.PI / 2;
      const lineCoord = [0, 0, radius * Math.cos(angle), radius * Math.sin(angle)];
      g.append("line")
        .attr("x1", lineCoord[0])
        .attr("y1", lineCoord[1])
        .attr("x2", lineCoord[2])
        .attr("y2", lineCoord[3])
        .attr("stroke", "grey")
        .attr("stroke-width", 1);

      // Labels
      g.append("text")
        .attr("x", (radius + 10) * Math.cos(angle))
        .attr("y", (radius + 10) * Math.sin(angle))
        .text(d.axis)
        .style("font-size", "12px")
        .attr("text-anchor", "middle")
        .attr("alignment-baseline", "middle");
    });
  }, []);

  return (
    <div>
      <h2 className="font-semibold mb-2">Radar Chart</h2>
      <svg ref={ref}></svg>
    </div>
  );
}
