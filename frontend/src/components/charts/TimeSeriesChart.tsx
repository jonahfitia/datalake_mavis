"use client";

import { useEffect, useRef } from "react";
import * as d3 from "d3";


interface DataPoint {
  date: Date;
  value: number;
}

export default function TimeSeriesChart() {
  const ref = useRef<SVGSVGElement | null>(null);

  useEffect(() => {
    if (!ref.current) return;

    
    const data: DataPoint[] = [
      { date: new Date("2025-07-01"), value: 120 },
      { date: new Date("2025-07-02"), value: 125 },
      { date: new Date("2025-07-03"), value: 130 },
      { date: new Date("2025-07-04"), value: 128 },
      { date: new Date("2025-07-05"), value: 122 },
      { date: new Date("2025-07-06"), value: 118 },
      { date: new Date("2025-07-07"), value: 126 },
    ];

    const svg = d3.select<SVGSVGElement, unknown>(ref.current);
    svg.selectAll("*").remove();

    const width: number = 600;
    const height: number = 400;
    const margin: { top: number; right: number; bottom: number; left: number } = {
      top: 20,
      right: 30,
      bottom: 50,
      left: 50,
    };

    
    const x = d3
      .scaleTime()
      .domain(d3.extent(data, (d: DataPoint) => d.date) as [Date, Date])
      .range([margin.left, width - margin.right]);

    const y = d3
      .scaleLinear()
      .domain([
        d3.min(data, (d: DataPoint) => d.value)! - 10,
        d3.max(data, (d: DataPoint) => d.value)! + 10,
      ])
      .range([height - margin.bottom, margin.top]);

    
    const line = d3
      .line<DataPoint>()
      .x((d) => x(d.date))
      .y((d) => y(d.value));

    
    svg
      .append("g")
      .attr("class", "x-axis")
      .attr("transform", `translate(0,${height - margin.bottom})`)
      .call(
        d3
          .axisBottom<Date>(x)
          .tickFormat((domainValue: Date, index: number) =>
            d3.timeFormat("%Y-%m-%d")(domainValue)
          )
      )
      .selectAll("text")
      .attr("transform", "rotate(-45)")
      .style("text-anchor", "end");

    svg
      .append("g")
      .attr("transform", `translate(${margin.left},0)`)
      .call(d3.axisLeft(y))
      .append("text")
      .attr("fill", "#000")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", "0.71em")
      .style("text-anchor", "end")
      .text("Blood Pressure (mmHg)");

    
    svg
      .append("path")
      .datum(data)
      .attr("class", "line")
      .attr("fill", "none")
      .attr("stroke", "steelblue")
      .attr("stroke-width", 2)
      .attr("d", line);

    
    const zoom = d3
      .zoom<SVGSVGElement, unknown>()
      .scaleExtent([1, 8])
      .translateExtent([
        [margin.left, margin.top],
        [width - margin.right, height - margin.bottom],
      ])
      .on("zoom", (event: d3.D3ZoomEvent<SVGSVGElement, unknown>) => {
        const newX = event.transform.rescaleX(x);

        
        svg
          .select<SVGGElement>(".x-axis")
          .call(
            d3
              .axisBottom<Date>(newX)
              .tickFormat((domainValue: Date) =>
                d3.timeFormat("%Y-%m-%d")(domainValue)
              )
          );

        
        const lineZoomed = d3
          .line<DataPoint>()
          .x((d) => newX(d.date))
          .y((d) => y(d.value));

        
        svg
          .select<SVGPathElement>(".line")
          .attr("d", lineZoomed(data)); 

        
        svg
          .selectAll<SVGCircleElement, DataPoint>(".dot")
          .attr("cx", (d) => newX(d.date));
      });


    svg.call(zoom);

    
    svg
      .selectAll<SVGCircleElement, DataPoint>(".dot")
      .data(data)
      .enter()
      .append("circle")
      .attr("class", "dot")
      .attr("cx", (d: DataPoint) => x(d.date))
      .attr("cy", (d: DataPoint) => y(d.value))
      .attr("r", 5)
      .attr("fill", "steelblue")
      .on("mouseover", function (event: MouseEvent, d: DataPoint) {
        d3.select(this).attr("r", 8);
        svg
          .append("text")
          .attr("class", "tooltip")
          .attr("x", x(d.date) + 10)
          .attr("y", y(d.value) - 10)
          .text(`${d.value} mmHg, ${d3.timeFormat("%Y-%m-%d")(d.date)}`);
      })
      .on("mouseout", function () {
        d3.select(this).attr("r", 5);
        svg.select(".tooltip").remove();
      });

  }, []);

  return (
    <div>
      <h2 className="font-semibold mb-2">Blood Pressure Time Series</h2>
      <svg ref={ref}></svg>
    </div>
  );
}