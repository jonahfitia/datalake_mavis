'use client'
import React, { useRef, useEffect, useMemo } from 'react'
import * as d3 from 'd3'

type Cell = { x: string; y: string; value: number }
type HeatmapProps = {
  width: number
  height: number
  data: Cell[]
}

export default function Heatmap({ width, height, data }: HeatmapProps) {
  const svgRef = useRef<SVGSVGElement>(null)

  // Précomputations via useMemo
  const xValues = useMemo(() => Array.from(new Set(data.map(d => d.x))), [data])
  const yValues = useMemo(() => Array.from(new Set(data.map(d => d.y))), [data])
  const colorScale = useMemo(() => {
    const vals = data.map(d => d.value)
    return d3.scaleSequential(d3.interpolateInferno)
      .domain([d3.min(vals) as number, d3.max(vals) as number])
  }, [data])

  const xScale = useMemo(() =>
    d3.scaleBand().domain(xValues).range([0, width]).padding(0.05),
    [xValues, width]
  )
  const yScale = useMemo(() =>
    d3.scaleBand().domain(yValues).range([0, height]).padding(0.05),
    [yValues, height]
  )

  useEffect(() => {
    if (!svgRef.current) return
    const svg = d3.select(svgRef.current)
    svg.selectAll('rect').remove() // очистка ancienne visualisation

    svg.attr('width', width).attr('height', height)

    svg.selectAll('rect')
      .data(data)
      .enter()
      .append('rect')
      .attr('x', d => xScale(d.x)!)
      .attr('y', d => yScale(d.y)!)
      .attr('width', xScale.bandwidth())
      .attr('height', yScale.bandwidth())
      .attr('fill', d => colorScale(d.value))
      .attr('stroke', '#fff')
  }, [data, width, height, xScale, yScale, colorScale])

  return (
    <div className="overflow-auto">
      <svg ref={svgRef} className="shadow rounded bg-white" />
    </div>
  )
}
