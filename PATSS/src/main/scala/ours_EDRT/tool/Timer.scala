package ours_EDRT.tool

class Timer extends Serializable {
  private var time:Long=0
  def restart()={
    time=System.currentTimeMillis()
  }
  def elapsed() ={
    (System.currentTimeMillis()-time)/1000.0
  }
}




