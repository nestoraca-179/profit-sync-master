# modulos
import client
import invoice
import sale_doc
import buy_doc
import order
import reng_invoice
import collect
import reng_collect
import messages as msg
import sync_manager as sm

# librerias
import time
import pyodbc
import threading
from datetime import datetime

# variables
items_total_inv_v = []
items_saldo_doc_v = []
items_total_inv_c = []
items_saldo_doc_c = []

# conexiones
connect_main = {
    "server": "172.16.10.20\SQLS2014STD",
    "database": "DEMOA",
    "username": "sa",
    "password": "Soporte123456"
}

connect_vln = {
    "server": "IT-MOV-91\SQLS2014SE",
    "database": "DEMOA",
    "username": "sa",
    "password": "Soporte123456"
}

connect_mar = {
    "server": "AC-10\SQLS2014SE",
    "database": "DEMOA",
    "username": "sa",
    "password": "Soporte123456"
}

# TIMER
def timer(timer_runs):
    i = 0

    while timer_runs.is_set():
        i = i + 1
        now = datetime.now()
        main()

        with open('logs.txt', 'a') as f:
            f.write(f"{i} -> {now.strftime('%d/%m/%Y %H:%M:%S')}\n")
            
        time.sleep(180)

# GET CONNECTION
def get_connection(suc: str):

    conn = {}

    if suc == "02":
        conn = connect_vln
    elif suc == "03":
        conn = connect_mar

    return conn

# MAIN
def main():
    try:
        # iniciando conexiones a sync y main
        sync_manager = sm.SyncManager()
        con_main = pyodbc.connect(f'DRIVER={{SQL Server}}; SERVER={connect_main["server"]}; DATABASE={connect_main["database"]}; UID={connect_main["username"]}; PWD={connect_main["password"]}')
        # conexion exitosa
        msg.print_connection_success()
    except:
        # conexion fallida
        msg.print_connection_error()
        pass
    else:
        # cursor para la DB main
        cursor_main = con_main.cursor()

        # consultando los items para DELETE
        itemsDel = sync_manager.get_items_delete()

        if len(itemsDel) > 0:
            for item in itemsDel:

                connect_sec = get_connection(item.Sucursal.rstrip())

                if item.Tipo == "CLI": # CLIENTE
                    
                    result = client.delete_client(item, connect_sec)
                    msg.print_msg_result_delete('Cliente', item.ItemID, 'o', result)

                    if result == 1 or result == 2:
                        sync_manager.update_item('ItemsEliminar', item.ID)

                elif item.Tipo == "FV": # FACTURA VENTA

                    result = invoice.delete_invoice(item, connect_sec)
                    msg.print_msg_result_delete('Factura', item.ItemID, 'a', result)

                    if result == 1 or result == 2:
                        sync_manager.update_item('ItemsEliminar', item.ID)

                elif item.Tipo == "FVR": # FACTURA VENTA RENGLON

                    index = str.rfind(item.ItemID, '-')
                    fact = item.ItemID[0:index]
                    reng = item.ItemID[index + 1:]

                    result = reng_invoice.delete_reng_invoice(fact, reng, connect_sec)
                    msg.print_msg_result_delete(f'Renglon N° {reng} de la factura', fact, 'o', result)

                    if result == 1 or result == 2:
                        sync_manager.update_item('ItemsEliminar', item.ID)

                elif item.Tipo == "COB": # COBRO

                    result = collect.delete_collect(item, connect_sec)
                    msg.print_msg_result_delete('Cobro', item.ItemID, 'o', result)

                    if result == 1 or result == 2:
                        sync_manager.update_item('ItemsEliminar', item.ID)

                elif item.Tipo == "COBTR": # COBRO TP RENGLON

                    index = str.rfind(item.ItemID, '-')
                    cob = item.ItemID[0:index]
                    reng = item.ItemID[index + 1:]

                    c = collect.search_collect(cursor_main, cob)

                    if c is None: # El cobro no esta en la base principal
                        msg.print_item_not_found('El renglón TP no puede ser eliminado ya que el cobro', cob)
                        sync_manager.delete_item('ItemsEliminar', item.ID)
                    else:

                        result = reng_collect.delete_reng_tp_collect(cob, reng, connect_sec)
                        msg.print_msg_result_delete(f'Renglon TP N° {reng} del cobro', cob, 'o', result)

                        if result == 1 or result == 2:
                            sync_manager.update_item('ItemsEliminar', item.ID)

                elif item.Tipo == "PV": # PEDIDO VENTA

                    result = order.delete_order(item, connect_sec)
                    msg.print_msg_result_delete('Pedido', item.ItemID, 'o', result)

                    if result == 1 or result == 2:
                        sync_manager.update_item('ItemsEliminar', item.ID)
        else:
            msg.print_no_items_to_delete()

        # consultando los items para UPDATE
        itemsMod = sync_manager.get_items_update()

        if len(itemsMod) > 0:
            for item in itemsMod:

                connect_sec = get_connection(item.Sucursal.rstrip())

                if item.Tipo == "CLI": # CLIENTE

                    c = client.search_client(cursor_main, item.ItemID)

                    if c is None:
                        msg.print_item_not_found('El cliente', item.ItemID)
                        sync_manager.delete_item('ItemsModificar', item.ID)
                    else:

                        result = client.update_client(item, connect_sec)
                        msg.print_msg_result_update('Cliente', item.ItemID, item.CampoModificado, 'o', result)

                        if result == 1:
                            sync_manager.update_item('ItemsModificar', item.ID)

                elif item.Tipo == "FV": # FACTURA VENTA

                    i = invoice.search_invoice(cursor_main, item.ItemID)

                    if i is None:
                        msg.print_item_not_found('La factura', item.ItemID)
                        sync_manager.delete_item('ItemsModificar', item.ID)
                    else:

                        if item.CampoModificado == "total_neto" and item.AntiguoValor > item.NuevoValor:
                            items_total_inv_v.append(item)
                        else:

                            result = invoice.update_invoice(item, connect_sec)
                            msg.print_msg_result_update('Factura', item.ItemID, item.CampoModificado, 'a', result)

                            if result == 1:
                                sync_manager.update_item('ItemsModificar', item.ID)            

                elif item.Tipo == "FVR": # FACTURA VENTA RENGLON

                    index = str.rfind(item.ItemID, '-')
                    fact = item.ItemID[0:index]
                    reng = item.ItemID[index + 1:]

                    i = invoice.search_invoice(cursor_main, fact)

                    if i is None:
                        msg.print_item_not_found('La factura', fact)
                        sync_manager.delete_item('ItemsModificar', item.ID)
                    else:
                        
                        result = reng_invoice.update_reng_invoice(item, fact, reng, connect_sec)
                        msg.print_msg_result_update(f'Renglon N° {reng} de la factura', fact, item.CampoModificado, 'o', result)

                        if result == 1:
                            sync_manager.update_item('ItemsModificar', item.ID)
        
                elif item.Tipo == "DVF": # DOCUMENTO VENTA FACTURA

                    d = sale_doc.search_sale_doc(cursor_main, 'FACT', item.ItemID)

                    if d is None:
                        msg.print_item_not_found('Documento de venta de la factura', item.ItemID)
                        sync_manager.delete_item('ItemsModificar', item.ID)
                    else:

                        if item.CampoModificado == "saldo" and item.AntiguoValor < item.NuevoValor:
                            items_saldo_doc_v.append(item)
                        else:

                            result = sale_doc.update_sale_doc(item, 'FACT', connect_sec)
                            msg.print_msg_result_update('Documento de venta de la factura', item.ItemID, item.CampoModificado, 'o', result)

                            if result == 1:
                                sync_manager.update_item('ItemsModificar', item.ID)

                elif item.Tipo == "ADE": # DOCUMENTO VENTA ADELANTO

                    d = sale_doc.search_sale_doc(cursor_main, 'ADEL', item.ItemID)

                    if d is None:
                        msg.print_item_not_found('Adelanto', item.ItemID)
                        sync_manager.delete_item('ItemsModificar', item.ID)
                    else:

                        result = sale_doc.update_sale_doc(item, 'ADEL', connect_sec)
                        msg.print_msg_result_update('Adelanto', item.ItemID, item.CampoModificado, 'o', result)

                        if result == 1:
                            sync_manager.update_item('ItemsModificar', item.ID)

                elif item.Tipo == "COB": # COBRO

                    c = collect.search_collect(cursor_main, item.ItemID)

                    if c is None:
                        msg.print_item_not_found('Cobro', item.ItemID)
                        sync_manager.delete_item('ItemsModificar', item.ID)
                    else:
                        result = collect.update_collect(item, connect_sec)
                        msg.print_msg_result_update('Cobro', item.ItemID, item.CampoModificado, 'o', result)

                        if result == 1:
                            sync_manager.update_item('ItemsModificar', item.ID)

                elif item.Tipo == "COBDR": # COBRO DOC RENGLON

                    index = str.rfind(item.ItemID, '-')
                    cob = item.ItemID[0:index]
                    reng = item.ItemID[index + 1:]

                    c = collect.search_collect(cursor_main, cob)

                    if c is None:
                        msg.print_item_not_found('El cobro', cob)
                        sync_manager.delete_item('ItemsModificar', item.ID)
                    else:

                        result = reng_collect.update_reng_doc_collect(item, cob, reng, connect_sec)
                        msg.print_msg_result_update(f'Renglon Doc. N° {reng} del cobro', cob, item.CampoModificado, 'o', result)

                        if result == 1:
                            sync_manager.update_item('ItemsModificar', item.ID)

                elif item.Tipo == "COBTR": # COBRO TP RENGLON

                    index = str.rfind(item.ItemID, '-')
                    cob = item.ItemID[0:index]
                    reng = item.ItemID[index + 1:]

                    c = collect.search_collect(cursor_main, cob)

                    if c is None:
                        msg.print_item_not_found('El cobro', cob)
                        sync_manager.delete_item('ItemsModificar', item.ID)
                    else:

                        result = reng_collect.update_reng_tp_collect(item, cob, reng, connect_sec)
                        msg.print_msg_result_update(f'Renglon TP N° {reng} del cobro', cob, item.CampoModificado, 'o', result)

                        if result == 1:
                            sync_manager.update_item('ItemsModificar', item.ID)

                elif item.Tipo == "PV": # PEDIDO VENTA

                    p = order.search_order(cursor_main, item.ItemID)

                    if p is None:
                        msg.print_item_not_found('El pedido', item.ItemID)
                        sync_manager.delete_item('ItemsModificar', item.ID)
                    else:

                        result = order.update_order(item, connect_sec)
                        msg.print_msg_result_update('Pedido', item.ItemID, item.CampoModificado, 'o', result)

                        if result == 1:
                            sync_manager.update_item('ItemsModificar', item.ID)

                elif item.Tipo == "PVR": # PEDIDO VENTA RENGLON

                    index = str.rfind(item.ItemID, '-')
                    ord = item.ItemID[0:index]
                    reng = item.ItemID[index + 1:]

                    p = order.search_order(cursor_main, ord)

                    if p is None:
                        msg.print_item_not_found('El pedido', ord)
                        sync_manager.delete_item('ItemsModificar', item.ID)
                    else:
                        
                        result = order.update_reng_order(item, ord, reng, connect_sec)
                        msg.print_msg_result_update(f'Renglon N° {reng} del pedido', ord, item.CampoModificado, 'o', result)

                        if result == 1:
                            sync_manager.update_item('ItemsModificar', item.ID)
            
            # modificando campo total neto en tabla factura de venta
            for total_inv in items_total_inv_v:
            
                result = invoice.update_sale_invoice(total_inv, 'FACT', connect_sec)
                msg.print_msg_result_update('Factura', total_inv.ItemID, total_inv.CampoModificado, 'a', result)

                if result == 1:
                    sync_manager.update_item('ItemsModificar', total_inv.ID)
            
            # modificando campo saldo en tabla documento de venta
            for saldo_doc in items_saldo_doc_v:
            
                result = sale_doc.update_sale_doc(saldo_doc, 'FACT', connect_sec)
                msg.print_msg_result_update('Documento de venta de la factura', saldo_doc.ItemID, saldo_doc.CampoModificado, 'o', result)

                if result == 1:
                    sync_manager.update_item('ItemsModificar', saldo_doc.ID)

            # modificando campo total neto en tabla factura de compra
            for total_inv in items_total_inv_c:
            
                result = invoice.update_buy_invoice(total_inv, connect_sec)
                msg.print_msg_result_update('Factura', total_inv.ItemID, total_inv.CampoModificado, 'a', result)

                if result == 1:
                    sync_manager.update_item('ItemsModificar', total_inv.ID)

            # modificando campo saldo en tabla documento de compra
            for saldo_doc in items_saldo_doc_c:
            
                result = buy_doc.update_buy_doc(saldo_doc, 'FACT', connect_sec)
                msg.print_msg_result_update('Documento de compra de la factura', saldo_doc.ItemID, saldo_doc.CampoModificado, 'o', result)

                if result == 1:
                    sync_manager.update_item('ItemsModificar', saldo_doc.ID)
        else:
            msg.print_no_items_to_update()
        
        # consultando los items para INSERT
        itemsAdd = sync_manager.get_items_insert()

        if len(itemsAdd) > 0:
            for item in itemsAdd:

                connect_sec = get_connection(item.Sucursal.rstrip())

                if item.Tipo == "CLI": # CLIENTE
                    
                    # busca el cliente en la base principal
                    c = client.search_client(cursor_main, item.ItemID)

                    if c is None: # error si el cliente no esta en la base principal
                        msg.print_item_not_found('El cliente', item.ItemID)
                        sync_manager.delete_item('ItemsAgregar', item.ID)
                    else:

                        # se intenta registrar el cliente
                        result = client.insert_client(c, connect_sec)
                        msg.print_msg_result_insert('Cliente', item.ItemID, 'o', result)

                        # se actualiza el registro en profit sync
                        if result == 1 or result == 2:
                            sync_manager.update_item('ItemsAgregar', item.ID)

                elif item.Tipo == "FV": # FACTURA VENTA

                    # busca la factura en la base principal
                    i = invoice.search_invoice(cursor_main, item.ItemID)

                    if i is None: # error si la factura no esta en la base principal
                        msg.print_item_not_found('La factura', item.ItemID)
                        sync_manager.delete_item('ItemsAgregar', item.ID)
                    else:

                        items = reng_invoice.search_all_invoice_items(cursor_main, item.ItemID) # items de la factura
                        doc = sale_doc.search_sale_doc(cursor_main, 'FACT', item.ItemID) # documento de venta de la factura

                        # se intenta registrar la factura
                        result = invoice.insert_invoice(i, items, doc, connect_sec)
                        msg.print_msg_result_insert('Factura', item.ItemID, 'a', result)

                        # se actualiza el registro en profit sync
                        if result == 1 or result == 2:
                            sync_manager.update_item('ItemsAgregar', item.ID)

                elif item.Tipo == "FVR": # FACTURA VENTA RENGLON

                    # busca nro de la factura y nro de renglon
                    index = str.rfind(item.ItemID, '-')
                    fact = item.ItemID[0:index]
                    reng = item.ItemID[index + 1:]

                    # busca la factura y el renglon en la base principal
                    i = invoice.search_invoice(cursor_main, fact)
                    new_reng = reng_invoice.search_reng_invoice(cursor_main, fact, reng)

                    if i is None: # error si la factura no esta en la base principal
                        msg.print_item_not_found('La factura', fact)
                        sync_manager.delete_item('ItemsAgregar', item.ID)
                    elif new_reng is None: # error si el renglon no esta en la base principal
                        msg.print_item_not_found(f'El renglón {reng} de la factura', fact)
                        sync_manager.delete_item('ItemsAgregar', item.ID)
                    else:

                        # se intenta registrar el renglon
                        result = reng_invoice.insert_reng_invoice(new_reng, connect_sec)
                        msg.print_msg_result_insert(f'Renglon N° {reng} de la factura', fact, 'o', result)

                        # se actualiza el registro en profit sync
                        if result == 1 or result == 2:
                            sync_manager.update_item('ItemsAgregar', item.ID)
                
                elif item.Tipo == "COB": # COBRO

                    # busca el cobro en la base principal
                    cob = collect.search_collect(cursor_main, item.ItemID)

                    if cob is None: # error si el cobro no esta en la base principal
                        msg.print_item_not_found('El cobro', item.ItemID)
                        sync_manager.delete_item('ItemsAgregar', item.ID)
                    else:

                        rengs_doc = collect.search_rengs_doc_collect(cursor_main, item.ItemID) # items doc del cobro
                        rengs_tp = collect.search_rengs_tp_collect(cursor_main, item.ItemID) # items pago del cobro

                        # se intenta registrar el cobro
                        result = collect.insert_collect(cob, rengs_doc, rengs_tp, cursor_main, connect_sec)
                        msg.print_msg_result_insert('Cobro', item.ItemID, 'o', result)

                        # se actualiza el registro en profit sync
                        if result == 1 or result == 2:
                            sync_manager.update_item('ItemsAgregar', item.ID)

                elif item.Tipo == "COBTR": # COBRO TP RENGLON

                    # busca nro del cobro y nro de renglon
                    index = str.rfind(item.ItemID, '-')
                    cob = item.ItemID[0:index]
                    reng = item.ItemID[index + 1:]

                    # busca el cobro y el renglon en la base principal
                    c = collect.search_collect(cursor_main, cob)
                    new_reng = reng_collect.search_reng_tp_collect(cursor_main, cob, reng)
                    
                    if c is None: # error si la factura no esta en la base principal
                        msg.print_item_not_found('El cobro', cob)
                        sync_manager.delete_item('ItemsAgregar', item.ID)
                    elif new_reng is None: # error si el renglon no esta en la base principal
                        msg.print_item_not_found(f'El renglón {reng} del cobro', fact)
                        sync_manager.delete_item('ItemsAgregar', item.ID)
                    else:
                        
                        # se intenta registrar el renglon
                        result = reng_collect.insert_reng_tp_collect(new_reng, connect_sec)
                        msg.print_msg_result_insert(f'Renglon TP N° {reng} del cobro', cob, 'o', result)

                        # se actualiza el registro en profit sync
                        if result == 1 or result == 2:
                            sync_manager.update_item('ItemsAgregar', item.ID)

                elif item.Tipo == "PV": # PEDIDO VENTA

                    # busca el pedido en la base principal
                    p = order.search_order(cursor_main, item.ItemID)

                    if p is None: # error si el pedido no esta en la base principal
                        msg.print_item_not_found('El pedido', item.ItemID)
                        sync_manager.delete_item('ItemsAgregar', item.ID)
                    else:

                        items = order.search_all_order_items(cursor_main, item.ItemID) # items del pedido

                        # se intenta registrar el pedido
                        result = order.insert_order(p, items, connect_sec)
                        msg.print_msg_result_insert('Pedido', item.ItemID, 'o', result)

                        # se actualiza el registro en profit sync
                        if result == 1 or result == 2:
                            sync_manager.update_item('ItemsAgregar', item.ID)
        else:
            msg.print_no_items_to_insert()
        
        # cerrando los cursores
        cursor_main.close()

    finally:
        # fin de la sincronizacion
        con_main.close()
        del sync_manager

        time.sleep(3)

main()
# timer_runs = threading.Event()
# timer_runs.set()
# t = threading.Thread(target=timer, args=(timer_runs,))
# t.start()