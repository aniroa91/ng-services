package churn.utils

object CommonUtil {

  val STATUS_MAP = Map(
    "BINH THUONG" -> 0,
    "HUY DICH VU" -> 1,
    "NVLDTT" -> 2,
    "CTBDV" -> 3,
    "KH dang chuyen dia diem".toUpperCase() -> 4,
    "Cho thanh ly".toUpperCase() -> 5,
    "Ngung vi tranh chap cuoc".toUpperCase() -> 6
  )

  val REGION = Map(
    "VUNG 1" -> 1,
    "VUNG 2" -> 2,
    "VUNG 3" -> 3,
    "VUNG 4" -> 4,
    "VUNG 5" -> 5,
    "VUNG 6" -> 6,
    "VUNG 7" -> 7
  )
}