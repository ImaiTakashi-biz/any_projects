from win32com.client import Dispatch

# 本番
acshukka = r"\\Landisk-6ac78a\共有\品質保証課\☆数値検査\数値検査記録.accdb"

accapp = Dispatch("Access.Application")
accapp.OpenCurrentDatabase(acshukka)

accapp.Visible = True

print(accapp.run("UpdateData"))
# Updatedata : Pythonからの呼び出しに使用するモジュール内のサブプロシージャ
# 返り値一覧(str型)
# "完了 : データ更新が完了しました"
# "失敗 : データ更新に失敗しました {エラー内容}"

accapp.quit()
