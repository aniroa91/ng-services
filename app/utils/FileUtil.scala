package utils


import java.io.File
import scala.util.Try
import java.nio.file.Files
import scala.reflect.io.Path
import java.io.BufferedWriter
import java.io.FileWriter
import scala.io.Source
import java.io.IOException

object FileUtil {

  /**
   * Read content form fileName
   */
  def readLines(fileName: String): List[String] = {
    val file = Source.fromFile(fileName)
    val res = file.getLines.toList
    // Close file before return result to avoid error "too many file opened"
    file.close()
    res
  }

  /**
   * Read content form file in resource path
   */
  def readResource(inPath: String): List[String] = {
    val in = FileUtil.getClass.getResourceAsStream(inPath)
    val buffer = scala.io.Source.fromInputStream(in)
    buffer.getLines().toList
  }

  /**
   * Write content to file.
   * If path to file don't exist => Exception: No such file or directory
   */
  def write(fileName: String, content: String) {
    try {
      val file = new File(fileName)
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(content)
      bw.close()
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }

  /**
   * Write content to file.
   * If path to file don't exist => Exception: No such file or directory
   */
  def write(fileName: String, content: Array[Byte]) {
    Files.write(new File(fileName).toPath(), content)
  }

  /**
   * Move from oldPath to newPath
   */
  def move(oldPath: String, newPath: String): Boolean = {
    Try(new File(oldPath).renameTo(new File(newPath))).getOrElse(false)
  }

  /**
   * Copy from oldPath to newPath
   */
  def copy(oldPath: String, newPath: String) = {
    Files.copy(new File(oldPath).toPath(), new File(newPath).toPath())
  }

  /**
   * Delete file or directory
   */
  def delete(filePath: String): Boolean = {
    val path: Path = Path(filePath)
    Try(path.deleteRecursively()).getOrElse(false)
  }

  /**
   * Append more text to end file.
   * Exception if file don't exist
   */
  def append(line: String, fileName: String) {
    val fw = new FileWriter(fileName, true)
    try {
      fw.write(line)
      fw.write("\n")
    } finally fw.close()
  }

  /**
   * Get all file in directory.
   * Return name fo File, don't full path
   */
  def getListOfFiles(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).map(x => x.getName).toList
    } else {
      List[String]()
    }
  }

  /**
   * Get all sub directory in directory.
   * Return name fo File, don't full path
   */
  def getListOfDir(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isDirectory).map(x => x.getName).toList
    } else {
      List[String]()
    }
  }

  /**
   * Get all file and sub directory in directory.
   * Return name fo File, don't full path
   */
  def getList(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.map(x => x.getName).toList
    } else {
      List[String]()
    }
  }

  def isExist(path: String): Boolean = {
    new java.io.File(path).exists
  }

  def getFilesMatchingRegex(dir: String, regex: util.matching.Regex) = {
    new java.io.File(dir).listFiles
      .filter(file => regex.findFirstIn(file.getName).isDefined)
  }

}
