"use client";

import { useEffect, useRef } from "react";
import * as d3 from "d3";

export default function ChartHeatmap() {
  const ref = useRef<SVGSVGElement | null>(null);

  useEffect(() => {
    if (!ref.current) return;

    const svg = d3.select(ref.current);
    svg.selectAll("*").remove(); // Nettoyage

    const width = 300;
    const height = 200;
    const cols = 6;
    const rows = 4;
    const cellWidth = width / cols;
    const cellHeight = height / rows;

    // Données aléatoires (ou statiques si tu préfères)
    const data = Array.from({ length: rows * cols }, (_, i) => ({
      x: i % cols,
      y: Math.floor(i / cols),
      value: Math.floor(Math.random() * 100), // valeur entre 0 et 100
    }));

    // Scales
    const colorScale = d3
      .scaleSequential(d3.interpolateYlOrRd)
      .domain([0, 100]);

    // Création du svg
    svg.attr("width", width).attr("height", height);

    svg
      .selectAll("rect")
      .data(data)
      .enter()
      .append("rect")
      .attr("x", d => d.x * cellWidth)
      .attr("y", d => d.y * cellHeight)
      .attr("width", cellWidth)
      .attr("height", cellHeight)
      .attr("fill", d => colorScale(d.value))
      .attr("stroke", "#ccc");

    // Texte en option : valeur dans chaque cellule
    svg
      .selectAll("text")
      .data(data)
      .enter()
      .append("text")
      .attr("x", d => d.x * cellWidth + cellWidth / 2)
      .attr("y", d => d.y * cellHeight + cellHeight / 2)
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .attr("fill", "black")
      .attr("font-size", "10px")
      .text(d => d.value);
  }, []);

  return (
    <div>
      <h2 className="font-semibold mb-2">Heatmap</h2>
      <svg ref={ref}></svg>
    </div>
  );
}
