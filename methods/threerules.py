from datetime import datetime, timedelta

#ФОРМАТ: сколько, операция в админке, кол-во в админке
def threshold_rule(price,operation,number):
    price = float(price)
    number = float(number)
    expression = f'{price} {operation} {number}'
    return bool(eval(expression))

#ФОРМАТ: кому, сколько, операция, сумма операции , временное окно, кол-во операций, данные
def pattern_rule(receiver, money, pat_oper, pat_quant, window,time_t, oper_quant, data_for_this_oper):
    if time_t == "minutes": td = timedelta(minutes=window)
    elif time_t == "hours": td = timedelta(hours=window)
    elif time_t == "days": td = timedelta(days=window)
    else: td = timedelta(minutes=window)
    window_start = datetime.now() - td
    ed = 0

    for search in data_for_this_oper:
        try:
            ts = datetime.fromisoformat(search["timestamp"].replace("Z", "+00:00")) if isinstance(search["timestamp"], str) else search["timestamp"]
            amount = float(search["amount"])
            recv = str(search["receiver_account"])

            if window_start <= ts <= datetime.now() and recv == str(receiver) and threshold_rule(amount, pat_oper, pat_quant):
                ed += 1
        except Exception as e:
            print(f"[pattern_rule] Пропуск записи {search}: {e}")
            continue

    return ed >= oper_quant

#ФОРМАТ: булевое выражение, денег заплачено, время операции    
def composite_rule(bulev,amount,time):
    bulev = str(bulev).replace(" ","").replace("amount",str(amount)).replace("AND","and").replace("OR","or").replace("NOT","not")
    if "00:00:00"<=time.split()[1]<="06:00:00": bulev = bulev.replace("nighttime","True").replace("daytime","False")
    else: bulev = bulev.replace("nighttime","False").replace("daytime","True")
    return eval(bulev)

'''
operations = [">=",">","<=","<","==","!="] # в панели ≥, >, ≤, <, =, !=

DATA = [("2025.10.19 00:00:09",111111,222222,12000),("2025.10.19 15:06:12",111111,222222,12000),("2025.10.19 15:06:30",111111,222222,12000)]
#ФОРМАТ : дата,время,кто,кому,сколько

admin_panel = [0,10000,60,2,2,2,15000,"(amount > 5 000) AND (nighttime)"]
#ФОРМАТ :  threshold_operation, quantity, time_window, operations_quantity, pattern operation, pattern_quantity, ед. изм. времени,композитка

time_types = ["minutes","hours","days"]


threshold_operation = admin_panel[0] # значение получается от выбора в панельке(от 0 до 5)
threshold_quantity = str(admin_panel[1]) # значение получается из поля


time_window = admin_panel[2] # окно для pattern
time_type = time_types[admin_panel[3]]
operations_quantity = admin_panel[4] # кол-во операций
pattern_operation = admin_panel[5] # для проверки треша(знак)
pattern_quantity = str(admin_panel[6]) # для проверки треша(кол-во)

composite_bulev = admin_panel[7]

for i in range(0,len(DATA)):
    threshold_ans = threshold_rule(str(DATA[i][3]), operations[threshold_operation], threshold_quantity)
    #ФОРМАТ: сколько, операция в админке, кол-во в админке
    pattern_ans = pattern_rule(DATA[i][2], DATA[i][3], pattern_operation, pattern_quantity, time_window,time_type, operations_quantity, DATA[0:i+1])
    #ФОРМАТ: кому,сколько, операция, сумма операции , временное окно, кол-во операций, данные
    composite_ans = composite_rule(composite_bulev,DATA[i][3],DATA[i][0])
    print(threshold_ans, pattern_ans,composite_ans)
'''
