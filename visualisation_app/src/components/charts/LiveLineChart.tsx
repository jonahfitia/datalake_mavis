"use client";

import { useRef, useEffect, useState } from "react";
import * as d3 from "d3";

export default function LiveLineChart() {
  const ref = useRef<SVGSVGElement | null>(null);
  const [data, setData] = useState(
    Array.from({ length: 20 }, () => Math.random() * 100)
  );

  useEffect(() => {
    if (!ref.current) return;

    const svg = d3.select(ref.current);
    const width = 500;
    const height = 200;
    const margin = { top: 20, right: 20, bottom: 30, left: 40 };

    svg.attr("width", width).attr("height", height);

    const xScale = d3
      .scaleLinear()
      .domain([0, data.length - 1])
      .range([margin.left, width - margin.right]);

    const yScale = d3
      .scaleLinear()
      .domain([0, 100])
      .range([height - margin.bottom, margin.top]);

    // Spécifie que les données sont number[] (valeurs)
    const line = d3
      .line<number>()
      .x((_, i) => xScale(i))
      .y((d) => yScale(d))
      .curve(d3.curveMonotoneX);

    svg.selectAll("*").remove();

    svg
      .append("path")
      .datum(data)
      .attr("fill", "none")
      .attr("stroke", "steelblue")
      .attr("stroke-width", 2)
      .attr("d", line);

    const xAxis = d3.axisBottom(xScale).ticks(data.length);
    const yAxis = d3.axisLeft(yScale);

    svg
      .append("g")
      .attr("transform", `translate(0,${height - margin.bottom})`)
      .call(xAxis);

    svg.append("g").attr("transform", `translate(${margin.left},0)`).call(yAxis);
  }, [data]);

  useEffect(() => {
    const interval = setInterval(() => {
      setData((oldData) => {
        const newData = oldData.slice(1);
        newData.push(Math.random() * 100);
        return newData;
      });
    }, 1000);

    return () => clearInterval(interval);
  }, []);

  return <svg ref={ref}></svg>; // <- correction ici (pas svgRef)
}
