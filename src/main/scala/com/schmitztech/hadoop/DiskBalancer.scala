package com.schmitztech.hadoop

import java.io.File
import org.apache.commons.io.FileUtils

class DiskBalancer(directories: Seq[DataDirectory]) {
  require(!directories.isEmpty)

  val rand = new java.util.Random

  def status: String = 
    directories.map { dir =>
      s"${dir.file.getPath}:\n" +
      s"\t${dir.blocks.size} blocks\n" +
      s"\tBlocks: ${dir.humanSize}\n" +
      s"\tFree s: ${dir.humanFreeSpaceOnDisk}\n"
    }.mkString("\n")

  // variance in GB
  def variance(): Double = {
    val sizes = directories.map(_.freeSpaceOnDisk.toDouble / 1024.toDouble)
    val mean = sizes.sum.toDouble / sizes.size.toDouble

    sizes.map { size => 
      val diff = size - mean
      diff * diff
    }.sum.toDouble / sizes.size.toDouble
  }

  def step(): Boolean = {
    val initialVariance = variance()

    val emptiest = directories.maxBy(_.freeSpaceOnDisk)
    val fullest = directories.minBy(_.freeSpaceOnDisk)

    val blocks = fullest.blocks.toIndexedSeq
    val randomBlockIndex = rand.nextInt(blocks.size)
    val randomBlock = blocks(randomBlockIndex)

    fullest.move(randomBlock, emptiest)

    val finalVariance = variance()

    println(initialVariance + " -> " + finalVariance)

    finalVariance < initialVariance
  }

  def run() {
    val br = new java.io.BufferedReader(new java.io.InputStreamReader(System.in))
    while (step()) {
      println(status)
      println(variance)
    }
  }
}

object DiskBalancerMain extends App {
  val disks = args

  println("Creating balancer...")
  val balancer = new DiskBalancer(disks.map(path => DataDirectory(new File(path))))
  println(balancer.status)
  println(balancer.variance)

  println("Running program...")
  balancer.run()
}
