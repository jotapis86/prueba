#!/usr/bin/python 
# -*- coding: cp1252 -*-

from SimpleXMLRPCServer import *
from conexionBD import *
import os
#import math
import threading

import subprocess
import shutil


# ---
# Definiciones
# ---

# Tipo de Pagos
__EFECTIVO__ = 1
__TARJETA_DEBITO__ = 2
__TARJETA_CREDITO__ = 3
__CREDITO__ = 4

# Estado de Ventas
__PAGADA__ = "Pagada"
__PENDIENTExPAGAR__ = "Pendiente"

class servidorCentral:
    def __init__(self):
        """Constructor del servidor central. Inicia la conexion con la base de datos"""
        self.conexion = ConexionBD("bd")
        self.conexion.conectar()
        self.hacerBackup(os.getcwd(), "bd", "C:\\DropBox")
        self._lockIngresarVenta = threading.RLock()
        self._lockInsertarAbono = threading.RLock()
        self._lockInsertarCambio = threading.RLock()
        print "servidorCentral inicializado"


    def __del__(self):
        """Destructor del servidor central. Hace la desconexión de la base de datos."""
        self.conexion.desconectar()
        print "servidorCentral terminado"


    def hacerBackup(self, rutaOrigen, archivo, rutaDestino):
        """Hace backup de la bd copiandola al directorio DropBox (rutaDestino)."""
        try:
            shutil.copy2(archivo, rutaDestino)
            print "Backup realizado exitosamente!"
        except Exception, e:
            print "hacerBackup - Error >> ", e
            
        

    def getColaboradores(self):
        """Retorna los colaboradores"""
        return self.conexion.ejecutarSQL("select usuario,contraseña,primer_nombre,segundo_nombre,primer_apellido,segundo_apellido,fecha_ingreso,direccion,telefono,login from colaboradores")


    def getColaboradoresTipo(self, tipo):
        """Retorna todos los colaboradores del tipo entrado. Por ejemplo, si tipo=3, retorna solo los vendedores."""
        return self.conexion.ejecutarSQL("select c.usuario from colaboradores c, colaboradoresXtipoColaboradores cXt where c.usuario=cXt.usuario_Colaborador and cXt.id_TipoColaborador = %s"%(tipo))


    def getClaveColaborador(self, colaborador):
        """Retorna la clave del colaborador entrado."""
        return self.conexion.ejecutarSQL("select contraseña from colaboradores where usuario = '%s'"%(colaborador))[0][0]


    def getTipoColaborador(self, colaborador):
        """Retorna los perfiles activados para el colaborador entrado."""
        return self.conexion.ejecutarSQL("select id_TipoColaborador from colaboradoresXtipoColaboradores where usuario_colaborador = '%s'"%(colaborador))


    def getDatosBasicosCliente(self, idCliente):
        """Retorna los datos basicos del cliente entrado."""
        return self.conexion.ejecutarSQL("select id_tipoIdentificacion, id, nombres, primer_apellido from clientes where id = '%s' and activo='SI' "%(idCliente))


    def getTipoPagos(self):
        """Retorna los tipos de pagos disponibles en el sistema."""
        return self.conexion.ejecutarSQL("select tipo from tipoPagos")


    def getInfoProducto(self, cod_barras):
        """Retorna la información del producto identificado con el código de barras ingresado."""
        return self.conexion.ejecutarSQL("select * from productos where codigo = '%s'"%(cod_barras))


    def getCantidadDisponibleProducto(self, cod_barras):
        """Retorna la cantidad disponible del producto solicitado."""
        return self.conexion.ejecutarSQL("select cantidad_disponible from productos where codigo = '%s'"%(cod_barras))[0][0]

    
    def getIdTipoPago(self, descTipoPago):
        """Retorna el id del tipo de pago entrado"""
        return self.conexion.ejecutarSQL("select id from TipoPagos where tipo='%s'"%(descTipoPago))[0][0]


    def estadoVentaDadoTipoPago(self, idTipoPago):
        """Retorna el estado de la venta dado el tipo de pago"""
        if idTipoPago == __CREDITO__:
            return __PENDIENTExPAGAR__
        else:
            return __PAGADA__


    def getIdUltimaVenta(self):
        """Retorna el identificador de la ultima venta hecha"""
        r = self.conexion.ejecutarSQL("select max(id) from Ventas")
        if r != []:
            return r[0][0]
        else:
            return 0


    def ingresarVenta(self, total, tipoPago, idCliente, usuarioColaborador, listaProductos):
        """Ingresa una venta y registra todos los productos vendidos. Retorna True si es exitosa, False en otro caso."""
        self._lockIngresarVenta.acquire()
        try:
            # agrupar totales vendidos de cada cod_barras
            numVendidosXcod = dict()
            # calcular subtotal, totalIVA basado en IVA y valorTotal de cada producto en lista. Subtotal=total-totalIVA
            totalIVA = 0
            for (cod_barras, desc, cantidad, valorUnitario, IVA, valorTotal) in listaProductos:
                totalIVA += float(valorTotal) * float(IVA) / 100
                if numVendidosXcod.has_key(cod_barras):
                    numVendidosXcod[cod_barras] += float(cantidad)
                else:
                    numVendidosXcod[cod_barras] = float(cantidad)
            # almacena subtotal en variable
            subtotal = total - totalIVA 
            # revisar si existen las cantidades disponibles de cada codigo
            for cod, cant in numVendidosXcod.iteritems():
                cantidadDisponible = self.getInfoProducto(cod)[0][2]
                if cant > cantidadDisponible:
                    return (False,"Cantidades insuficientes del producto %s. Cantidad disponible: %s y Cantidad en Venta: %s"%(cod,cantidadDisponible,cant),-1)
            # obtener id_TipoPago dependiendo del tipo de pago
            id_TipoPago = self.getIdTipoPago(tipoPago)
            # obtener estado de la venta dependiendo del tipo de pago
            estado = self.estadoVentaDadoTipoPago(id_TipoPago)
            # ingresar la venta en Ventas        
            if estado == __PAGADA__:
                self.conexion.ejecutarSQL("insert into Ventas (fecha,hora,subtotal,totalIVA,total,estado,usuario_Colaborador,id_Cliente,id_TipoPago,fechaPagoTotal,horaPagoTotal) values (DATE('now','localtime'),TIME('now','localtime'),%s,%s,%s,'%s','%s','%s',%s,DATE('now','localtime'),TIME('now','localtime'))"%(0,0,0,estado,usuarioColaborador,idCliente,id_TipoPago))
            else:
                self.conexion.ejecutarSQL("insert into Ventas (fecha,hora,subtotal,totalIVA,total,estado,usuario_Colaborador,id_Cliente,id_TipoPago) values (DATE('now','localtime'),TIME('now','localtime'),%s,%s,%s,'%s','%s','%s',%s)"%(0,0,0,estado,usuarioColaborador,idCliente,id_TipoPago))
            # obtener id de última venta
            id_Venta = self.getIdUltimaVenta()
            # ingresar todos los productos de listaProductos en ProductosXVentas
            numItem = 1
            for (cod_barras, desc, cantidad, valorUnitario, IVA, valorTotal) in listaProductos:
                self.conexion.ejecutarSQL("insert into ProductosXVentas (numero_item,codigo_producto,id_venta,usuario_Colaborador,cantidad,valor_unitario,IVA,valor_total) values (%s,'%s',%s,'%s',%s,%s,%s,%s)"%(numItem,cod_barras,id_Venta,usuarioColaborador,cantidad,valorUnitario,IVA,valorTotal))

		#Por cada producto vendido, se debe actualizar la tabla kardex.
		cantidadKardex = cantidad
		idProductoKardex = cod_barras
		saldos = self.conexion.ejecutarSQL("""select saldo_cantidad, saldo_valor, costo_unitario from kardex
                                                    where codigo_Producto='%s'
                                                    order by fecha and hora"""%(idProductoKardex))
		if len(saldos) == 0:
		    v_unitarioKardex = valorUnitario
		    valor_TotalKardex = float(cantidadKardex)*float(v_unitarioKardex)
		    saldo_cantidadKardex = cantidadKardex
		    saldo_valorKardex = valor_TotalKardex
		else:
		    v_unitarioKardex = saldos[len(saldos)-1][2]
		    valor_TotalKardex = float(cantidadKardex)*float(v_unitarioKardex)
		    saldo_cantidadKardex = saldos[len(saldos)-1][0]-float(cantidadKardex)
		    saldo_valorKardex = saldos[len(saldos)-1][1]-valor_TotalKardex
		if float(saldo_cantidadKardex) != 0:
                    costo_unitarioKardex = saldo_valorKardex/float(saldo_cantidadKardex)
                else:
                    costo_unitarioKardex = saldo_valorKardex
		try:
		    self.conexion.ejecutarSQL("""insert into Kardex (codigo_Producto, fecha, hora, detalle,cantidad, valor_total,saldo_cantidad, saldo_valor, valor_unitario,costo_unitario)
                                                values ('%s',DATE('now','localtime'),TIME('now','localtime'),'Venta',%s,%s,%s,%s,%s,%s )"""
                                              %(idProductoKardex, cantidadKardex, valor_TotalKardex, saldo_cantidadKardex,
                                                saldo_valorKardex, v_unitarioKardex, costo_unitarioKardex))
		except Exception, e:
		    print "Kardex Venta: ", e
		    self.conexion.rollback()
		    return (False,str(e),-1)
	    
                numItem += 1    
            # comprometer
	    self.conexion.commit()
            return (True,"",id_Venta)
        except Exception, e:
            print "ingresarVenta excepcion: ", e
            self.conexion.rollback()
            return (False,str(e),-1)
        finally:
            self._lockIngresarVenta.release()


    def getVenta(self, id):
        """Retorna el detalle de una venta."""
        return self.conexion.ejecutarSQL("""select id, fecha, hora, subtotal, totalIVA, total, estado, usuario_Colaborador, id_Cliente, id_TipoPago
                                        from ventas
                                        where id = %s"""%(id))


    def getVentas(self, fechaInicio, fechaFin, usuarioColaborador=""):
        """Retorna las ventas hechas entre el rango de fechas dado por el usuario entrado o el total de ventas si usuarioColaborador=''"""
        if usuarioColaborador == "":
            return self.conexion.ejecutarSQL("select v.id, v.fecha, v.hora, v.subtotal, v.totalIVA, v.total, v.estado, v.usuario_Colaborador, \
                                             v.id_Cliente, v.id_TipoPago, tP.tipo from ventas v, tipoPagos tP where v.id_tipoPago=tP.id \
                                             and v.fecha between '%s' and '%s'" %(fechaInicio,fechaFin))
        else:
            return self.conexion.ejecutarSQL("select v.id, v.fecha, v.hora, v.subtotal, v.totalIVA, v.total, v.estado, v.usuario_Colaborador, \
                                             v.id_Cliente, v.id_TipoPago, tP.tipo from ventas v, tipoPagos tP where v.id_tipoPago=tP.id \
                                             and v.fecha between '%s' and '%s' \
                                             and usuario_colaborador='%s'" %(fechaInicio,fechaFin,usuarioColaborador))

    def getVentasPendientes(self, idVenta="", idCliente="", idAbono="",fecha=""):
        """Retorna las ventas pendientes buscando con idVenta o idCliente si son entrados."""
        if idVenta == "" and idCliente == "" and idAbono == "" and fecha == "":
            return self.conexion.ejecutarSQL("select v.id,v.fecha,v.total,tP.tipo,c.id, c.nombres || ' ' || c.primer_apellido from ventas v, tipoPagos tP, clientes c \
                                             where v.id_TipoPago=tP.id and v.id_Cliente=c.id and v.estado='Pendiente'")
        elif idVenta != "":
            return self.conexion.ejecutarSQL("select v.id,v.fecha,v.total,tP.tipo,c.id, c.nombres || ' ' || c.primer_apellido from ventas v, tipoPagos tP, clientes c \
                                             where v.id_TipoPago=tP.id and v.id_Cliente=c.id and \
                                             v.estado='Pendiente' and v.id=%s"%(idVenta))
        elif idCliente != "":
            return self.conexion.ejecutarSQL("select v.id,v.fecha,v.total,tP.tipo,c.id, c.nombres || ' ' || c.primer_apellido from ventas v, tipoPagos tP, clientes c \
                                             where v.id_TipoPago=tP.id and v.id_Cliente=c.id and \
                                             v.estado='Pendiente' and v.id_cliente='%s'"%(idCliente))
	elif idAbono != "":
	    return self.conexion.ejecutarSQL("select v.id,v.fecha,v.total,tP.tipo,c.id, c.nombres || ' ' || c.primer_apellido from ventas v, tipoPagos tP, clientes c, Abonos a\
                                             where v.id_TipoPago=tP.id and v.id_Cliente=c.id and v.estado='Pendiente'\
	                                     and a.id_venta=v.id and a.id=%s"%(idAbono))
	elif fecha != "":
	    return self.conexion.ejecutarSQL("select v.id,v.fecha,v.total,tP.tipo,c.id, c.nombres || ' ' || c.primer_apellido from ventas v, tipoPagos tP, clientes c \
                                             where v.id_TipoPago=tP.id and v.id_Cliente=c.id and v.estado='Pendiente' and\
	                                     v.fecha between '%s' and '%s'"%(fecha[0],fecha[1]))
        
            

    def getAbonosVenta(self, idVenta):
        """Retorna los abonos hechos a la venta entrada."""
        return self.conexion.ejecutarSQL("select a.id,a.fecha,a.hora,a.valor,a.usuario_colaborador,tP.tipo from abonos a, tipoPagos tP \
                                         where a.id_tipoPago=tP.id and a.id_venta=%s"%(idVenta))
    
    def modificarAbono(self,lstIds):
	"""Modifica un abono dado el id, se puede modificar el total del abono o eliminarlo"""
	modificar = lstIds[0]
	eliminar = lstIds[1]
	# Primero se modifica los abonos
	for i in modificar:
	    try:
		self.conexion.ejecutarSQL("update Abonos set valor=%s where id=%s"%(i[1],i[0]))
	    except Exception, e:
		print "modificarAbono excepcion: ", e
		self.conexion.rollback()
		return False
	# Ahora se eliminan los abonos
	for i in eliminar:
	    try:
		self.conexion.ejecutarSQL("delete from Abonos where id=%s"%(i))
	    except Exception, e:
		print "modificarAbono excepcion: ", e
		self.conexion.rollback()
		return False
	self.conexion.commit()
	return True

    def insertarAbono(self, idVenta, monto, tipoPago, usuario):
        """Inserta un abono a la venta idVenta por el monto dado."""
        self._lockInsertarAbono.acquire()
        try:
            id_TipoPago = self.getIdTipoPago(tipoPago)
            self.conexion.ejecutarSQL("""insert into abonos (fecha,hora,valor,id_venta,usuario_colaborador,id_TipoPago) values (
                                      DATE('now','localtime'),TIME('now','localtime'),%s,%s,'%s',%s)"""%(monto,idVenta,usuario,id_TipoPago))
            self.conexion.commit()
            id_abono,fecha,hora = self.getIdUltimoAbono()
            return (True, id_abono,fecha,hora)
        except Exception, e:
            print "ingresarAbono excepcion: ", e
            self.conexion.rollback()
            return (False, -1,-1,-1)
        finally:
            self._lockInsertarAbono.release()

    def getIdUltimoAbono(self):
        """Retorna el identificador del ultimo abono hecho"""
        r = self.conexion.ejecutarSQL("select max(id),fecha,hora from abonos")
        if r != []:
            return r[0]
        else:
            return [0,0,0]


    # -- -- --
    # Inicio Cambios
    # -- -- --

    def insertarCambio(self, codigo_Producto_entra, codigo_Producto_sale, id_Venta, excedente, usuario_Colaborador):
        """Inserta un cambio."""
        self._lockInsertarCambio.acquire()
        try:
            self.conexion.ejecutarSQL("""insert into cambios (fecha,hora,codigo_Producto_entra, codigo_Producto_sale, id_Venta, excedente, usuario_Colaborador) values ( 
                                      DATE('now','localtime'),TIME('now','localtime'),'%s','%s',%s,%s,'%s')"""
                                      %(codigo_Producto_entra, codigo_Producto_sale, id_Venta, excedente, usuario_Colaborador))
            self.conexion.commit()
            return True
        except Exception, e:
            print "ingresarCambio excepcion: ", e
            self.conexion.rollback()
            return False
        finally:
            self._lockInsertarCambio.release()


    def getIdUltimoCambio(self):
        """Retorna el identificador del ultimo cambio hecho. '0' si no hay cambios aun."""
        r = self.conexion.ejecutarSQL("select max(id) from cambios")
        if r != []:
            return r[0][0]
        else:
            return 0


    def getCambio(self, idCambio):
        """Retorna la info del cambio requerido."""
        return self.conexion.ejecutarSQL("""select id, fecha, hora, codigo_Producto_entra, codigo_Producto_sale, id_Venta, excedente, usuario_Colaborador
                                            from cambios
                                            where id = %s"""%(idCambio))
        

    def getCambios(self, fechaInicio, fechaFin, usuarioColaborador=""):
        """Retorna los cambios hechos entre el rango de fechas dado por el usuario entrado o el total de abonos."""
	if usuarioColaborador == "" and fechaInicio == "" and fechaFin == "":
	    return self.conexion.ejecutarSQL("""select id, fecha, hora, codigo_Producto_entra, codigo_Producto_sale, id_Venta, excedente, usuario_Colaborador
                                             from cambios""")
        elif usuarioColaborador == "":
            return self.conexion.ejecutarSQL("""select id, fecha, hora, codigo_Producto_entra, codigo_Producto_sale, id_Venta, excedente, usuario_Colaborador
                                             from cambios
                                             where fecha between '%s' and '%s'""" %(fechaInicio,fechaFin))
        else:
            return self.conexion.ejecutarSQL("""select id, fecha, hora, codigo_Producto_entra, codigo_Producto_sale, id_Venta, excedente, usuario_Colaborador
                                             from cambios
                                             where fecha between '%s' and '%s'
                                             and usuario_Colaborador = '%s'""" %(fechaInicio,fechaFin,usuarioColaborador))


    def getCambiosQafectanCaja(self, fechaInicio, fechaFin, usuarioColaborador=""):
        """Retorna los cambios que afectan caja (Cambios realizados en fecha diferente a la fecha de la venta)
            hechos entre el rango de fechas dado por el usuario entrado o el total de abonos."""
	if usuarioColaborador == "" and fechaInicio == "" and fechaFin == "":
	    return self.conexion.ejecutarSQL("""select c.id, c.fecha, c.hora, c.codigo_Producto_entra, c.codigo_Producto_sale, c.id_Venta, c.excedente, c.usuario_Colaborador
                                             from cambios c, ventas v
                                             where c.id_Venta = v.id
                                                   and c.fecha != v.fecha""")
        elif usuarioColaborador == "":
            return self.conexion.ejecutarSQL("""select c.id, c.fecha, c.hora, c.codigo_Producto_entra, c.codigo_Producto_sale, c.id_Venta, c.excedente, c.usuario_Colaborador
                                             from cambios c, ventas v
                                             where c.id_Venta = v.id
                                                   and c.fecha != v.fecha
                                                   and c.fecha between '%s' and '%s'""" %(fechaInicio,fechaFin))
        else:
            return self.conexion.ejecutarSQL("""select c.id, c.fecha, c.hora, c.codigo_Producto_entra, c.codigo_Producto_sale, c.id_Venta, c.excedente, c.usuario_Colaborador
                                             from cambios c, ventas v
                                             where c.id_Venta = v.id
                                                   and c.fecha != v.fecha
                                                   and c.fecha between '%s' and '%s'
                                                   and c.usuario_Colaborador = '%s'""" %(fechaInicio,fechaFin,usuarioColaborador))


    
    # -- -- --
    # Fin Cambios
    # -- -- --

	
    def getIdUltimaCompra(self):
	"""Retorna el identificador de la 'ultima compra hecha"""
        r = self.conexion.ejecutarSQL("select max(id) from Compras")
        if r != []:
            return r[0][0]
        else:
            return -1


    def actualizarEstadoVenta(self, idVenta, estado):
        """Actualiza el estado de la venta idVenta."""
        try:
            self.conexion.ejecutarSQL("update ventas set estado='%s' where id=%s"%(estado,idVenta))
            self.conexion.commit()
            return True
        except Exception, e:
            print "actualizarEstadoVenta excepcion: ", e
            self.conexion.rollback()
            return False

    def actualizarFechaPagoTotal(self, idVenta):
        """Actualiza la fecha y hora en que se realizo el pago total de la venta."""
        try:
            self.conexion.ejecutarSQL("update ventas set fechaPagoTotal = DATE('now','localtime'), horaPagoTotal = TIME('now','localtime') where id=%s"%(idVenta))
            self.conexion.commit()
            return True
        except Exception, e:
            print "actualizarFechaPagoTotal excepcion: ", e
            self.conexion.rollback()
            return False


    def getAbono(self, cod):
        """Retorna los datos basicos de un abono."""
        return self.conexion.ejecutarSQL("""select id, fecha, hora, valor, id_venta, usuario_colaborador, id_tipoPago
                                            from abonos
                                            where id = %s"""%(cod))

    def getAbonos(self, fechaInicio, fechaFin, usuarioColaborador=""):
        """Retorna los abonos hechos entre el rango de fechas dado por el usuario entrado o el total de abonos si usuarioColaborador=''"""
	if usuarioColaborador == "" and fechaInicio == "" and fechaFin == "":
	    return self.conexion.ejecutarSQL("select a.id, a.fecha, a.hora, a.valor, a.id_venta, a.usuario_Colaborador, a.id_TipoPago, tP.tipo\
                                             from abonos a, tipoPagos tP where a.id_tipoPago=tP.id")
        elif usuarioColaborador == "":
            return self.conexion.ejecutarSQL("select a.id, a.fecha, a.hora, a.valor, a.id_venta, a.usuario_Colaborador, a.id_TipoPago, tP.tipo\
                                             from abonos a, tipoPagos tP where a.id_tipoPago=tP.id \
                                             and a.fecha between '%s' and '%s'" %(fechaInicio,fechaFin))
        else:
            return self.conexion.ejecutarSQL("select a.id, a.fecha, a.hora, a.valor, a.id_venta, a.usuario_Colaborador, a.id_TipoPago, tP.tipo\
                                             from abonos a, tipoPagos tP where a.id_tipoPago=tP.id \
                                             and a.fecha between '%s' and '%s' \
                                             and usuario_colaborador='%s'" %(fechaInicio,fechaFin,usuarioColaborador))

    def getProductos(self, codigo, desc):
        """Retorna todos los productos si codigo y desc = ''. Si no, intenta buscar los productos que coincidan con el codigo y descripcion."""
        if codigo != "" and desc != "":
            return self.conexion.ejecutarSQL("select codigo, descripcion, cantidad_disponible, porcentaje_IVA, precio_venta, fecha_modificacion, hora_modificacion, usuario_colaborador \
                                             from productos \
                                             where codigo like '%%%s%%' and descripcion like '%%%s%%' \
                                             order by codigo"%(codigo,desc))
        elif codigo != "":
            return self.conexion.ejecutarSQL("select codigo, descripcion, cantidad_disponible, porcentaje_IVA, precio_venta, fecha_modificacion, hora_modificacion, usuario_colaborador \
                                             from productos \
                                             where codigo like '%%%s%%' \
                                             order by codigo"%(codigo))
        elif desc != "":
            return self.conexion.ejecutarSQL("select codigo, descripcion, cantidad_disponible, porcentaje_IVA, precio_venta, fecha_modificacion, hora_modificacion, usuario_colaborador \
                                             from productos \
                                             where descripcion like '%%%s%%' \
                                             order by codigo"%(desc))
        else:
            return self.conexion.ejecutarSQL("select codigo, descripcion, cantidad_disponible, porcentaje_IVA, precio_venta, fecha_modificacion, hora_modificacion, usuario_colaborador \
                                             from productos \
                                             order by codigo")

    def getInventario(self):
        "Retorna el inventario completo del almacen."
        # [codigo, descripcion, cantidad_disponible, costo_promedio, precio_venta]
        productos = self.getProductos("","")
        inventario = []
        # ir a buscar el costo promedio de cada producto en todas las compras (productosXcompras)
        for p in productos:
            prod = []
            prod.append(p[0])
            prod.append(p[1])
            prod.append(p[2])
            prod.append(self.getCostoPromedioCompras(p[0]))
            prod.append(p[4])
            inventario.append(prod)
        #print inventario
        return inventario


    def getCostoPromedioCompras(self, codigo):
        """Retorna el costo promedio ponderado entre todas las compras hechas de un codigo"""
        return self.conexion.ejecutarSQL("select sum(valor_total)/sum(cantidad) from productosXcompras where codigo_producto = '%s'"%(codigo))[0][0]
        

    def existeProducto(self, cod):
        """Devuelve true o false si el producto existe o no."""
        return self.conexion.ejecutarSQL("select codigo from productos where codigo='%s'"%(cod)) != []

    def guardarProducto(self, cod, desc, iva, precioVenta, usuario):
        """Si el producto existe, actualiza sus datos. Si es nuev, inserta el nuevo código de producto con la descripción, iva y precio de venta dados."""
        try:
            if self.existeProducto(cod):
                self.conexion.ejecutarSQL("update productos set descripcion='%s', porcentaje_iva=%s, precio_venta=%s, fecha_modificacion=DATE('now','localtime'), hora_modificacion=TIME('now','localtime'), usuario_colaborador='%s' \
                                          where codigo='%s'"%(desc,iva,precioVenta,usuario,cod))
            else:
                self.conexion.ejecutarSQL("insert into productos (codigo, descripcion, cantidad_disponible, porcentaje_iva, precio_venta, fecha_modificacion, hora_modificacion, usuario_colaborador) values \
                                          ('%s','%s',0,%s,%s,DATE('now','localtime'),TIME('now','localtime'),'%s')"%(cod,desc,iva,precioVenta,usuario))
            self.conexion.commit()
            return True
        except Exception, e:
            print "guardarProducto excepcion: ", e
            self.conexion.rollback()
            return False

    def eliminarProducto(self, cod):
        """ string --: bool
            Elimina el producto con el código entrado."""
        try:
            self.conexion.ejecutarSQL("delete from productos where codigo='%s'"%(cod))
            self.conexion.commit()
            return True
        except Exception, e:
            print "eliminarProducto excepcion: ", e
            self.conexion.rollback()
            return False

    ################################################################
    #################PROVEEDORES######################
    def getProveedores(self,id,pro,compra):
	if id == "" and pro == "" and compra == "":
            return self.conexion.ejecutarSQL("""
		select id, nombre, telefono, direccion\
		from Proveedores \
		order by nombre""")
	elif id != "":
	    return self.conexion.ejecutarSQL("""
		select id, nombre, telefono, direccion\
		from Proveedores where id = '%s'"""%(id))
	elif pro != "":
	    return self.conexion.ejecutarSQL("""select distinct(p.id), p.nombre, p.telefono, p.direccion
                                                from Proveedores p, Compras c, Productos pro, ProductosXCompras pxc
                                                where pro.codigo='%s'
                                                    and pxc.codigo_producto=pro.codigo
                                                    and pxc.id_compra=c.id
                                                    and c.id_Proveedor=p.id
                                                order by p.nombre"""%(pro))
	elif compra != "":
	    return self.conexion.ejecutarSQL("""
		select p.id, p.nombre, p.telefono, p.direccion\
		from Proveedores p, Compras c\
	        where c.id = c.id_Proveedor and c.id=%s\
		order by nombre"""%(compra))
	

    def existeProveedor(self, id):
        """Devuelve true o false si el proveedor existe o no."""
        return self.conexion.ejecutarSQL("select id from Proveedores where id='%s'"%(id)) != []
    
    def guardarProveedor(self, tipo_id,id, nombre, telefono, direccion,newid):
        """Si el proveedor existe, actualiza sus datos."""
        try:
            if self.existeProveedor(id):
                self.conexion.ejecutarSQL("update Proveedores set id_TipoIdentificacion=%s, id=%s, nombre='%s', telefono=%s, direccion='%s' \
                                          where id=%s"%(tipo_id,newid,nombre,telefono,direccion,id))
            else:
                self.conexion.ejecutarSQL("insert into Proveedores (id_TipoIdentificacion, id, nombre, telefono, direccion) values \
                                          (%s,%s,'%s',%s,'%s')"%(tipo_id,id,nombre.encode("latin-1"),telefono,direccion.encode("latin-1")))
            self.conexion.commit()
            return True
        except Exception, e:
            print "guardarProducto excepcion: ", e
            self.conexion.rollback()
            return False

    def Tipo_id_Proveedor(self,id):
        "Retorna el tipo de identificación del proveedor id"
        return self.conexion.ejecutarSQL("select id_TipoIdentificacion from Proveedores where id=%s"%(id))

    def EliminarProveedor(self, id):
        """ string --: bool
            Elimina el proveedor con el id entrado."""
        try:
            self.conexion.ejecutarSQL("delete from Proveedores where id=%s"%(id))
            self.conexion.commit()
            return True
        except Exception, e:
            print "eliminarProducto excepcion: ", e
            self.conexion.rollback()
            return False

    ################################################################
    #################CLIENTES######################

    def getClientes(self,id):
	if id== "":
	    return self.conexion.ejecutarSQL("""
		select id, nombres, primer_apellido, activo\
		from Clientes \
		order by primer_apellido""")
	elif id != "":
	    return self.conexion.ejecutarSQL("""
		select id, nombres, primer_apellido, activo\
		from Clientes where id = %s \
		order by primer_apellido"""%(id))

    def getDatosCliente(self,id):
        return self.conexion.ejecutarSQL("""
            select id_TipoIdentificacion, id, nombres, primer_apellido, segundo_apellido, telefono, celular, dir, email, fecha_nacimiento \
            from Clientes \
            where id='%s'"""%(id))

    def existeCliente(self, id):
        """Devuelve true o false si el cliente existe o no."""
        return self.conexion.ejecutarSQL("select id from Clientes where id='%s'"%(id)) != []


    def guardarCliente(self, tipo_id, id, nombres, apellido, apellido2, tel, cel, dir, email, fecha):
        """Si el cliente existe, actualiza sus datos."""
        try:
##            if self.existeCliente(id):
##                self.conexion.ejecutarSQL("update Clientes set id_TipoIdentificacion=%s, id=%s, primer_nombre='%s', segundo_nombre='%s', primer_apellido='%s', segundo_apellido='%s', razon_social='%s', fecha_nacimiento='%s' \
##                                          where id=%s"%(tipo_id,newId,nombre,nombre2,apellido,apellido2,razon_social, fecha,id))
##            else:
##                self.conexion.ejecutarSQL("insert into Clientes (id_TipoIdentificacion, id, primer_nombre, segundo_nombre, primer_apellido, segundo_apellido, razon_social, fecha_nacimiento,activo) values \
##                                          (%s,%s,'%s','%s','%s','%s','%s','%s','SI')"%(tipo_id,id,nombre,nombre2,apellido,apellido2,razon_social, fecha))
            self.conexion.ejecutarSQL("delete from clientes where id='%s'"%(id))
            self.conexion.ejecutarSQL("insert into clientes (id_TipoIdentificacion, id, nombres, primer_apellido, segundo_apellido, telefono, celular, dir, email, fecha_nacimiento, activo) values (%s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', 'SI')"%(tipo_id, id, nombres, apellido, apellido2, tel, cel, dir, email, fecha))
            self.conexion.commit()
            return True
        except Exception, e:
            print "guardarProducto excepcion: ", e
            self.conexion.rollback()
            return False

    def EliminarCliente(self,id):
        """ string --: bool
            Deshabilita el cliente con el id entrado. Se debe garantizar que el cliente no tenga deudas."""
        try:
            self.conexion.ejecutarSQL("update Clientes set activo='NO' where id=%s"%(id))
            self.conexion.commit()
            return True
        except Exception, e:
            print "eliminarProducto excepcion: ", e
            self.conexion.rollback()
            return False
    
    def EliminarCliente_verificando(self,id):
        """ string --: bool
            Deshabilita el cliente con el id entrado. Se debe garantizar que el cliente no tenga deudas."""
        try:
	    estado = self.conexion.ejecutarSQL("select v.estado, v.id from Ventas v, Clientes c\
	                    where v.id_cliente = c.id and c.id=%s"%(id))
	    if 'Pendiente' in [x[0] for x in estado]:
		return [False,"Cliente tiene deudas pendientes"]
            self.conexion.ejecutarSQL("update Clientes set activo='NO' where id=%s"%(id))
            self.conexion.commit()
            return [True]
        except Exception, e:
            print "eliminarCliente_verificando excepcion: ", e
            self.conexion.rollback()
            return [False,-1]

    ################################################################
    #################COMPRAS######################

    def getCompras(self,codProd,fecha,codProv):
        if codProd=="" and fecha=="" and codProv=="":
            return self.conexion.ejecutarSQL("""
            select c.id, p.nombre, c.fecha, c.hora
            from Compras c, Proveedores p 
            where c.id_Proveedor = p.id 
            order by c.fecha""")
        elif codProd != "":
            return self.conexion.ejecutarSQL("""
            select distinct(c.id), p.nombre, c.fecha, c.hora
            from Compras c, Proveedores p, ProductosXCompras pc, Productos pro
            where c.id_Proveedor = p.id and pc.id_compra = c.id and pc.codigo_producto = pro.codigo and pro.codigo = '%s'
            order by c.fecha"""%(codProd))
	elif codProv != "":
            return self.conexion.ejecutarSQL("""
            select distinct(c.id), p.nombre, c.fecha, c.hora
            from Compras c, Proveedores p
            where c.id_Proveedor = p.id and p.id = '%s'
            order by c.fecha"""%(codProv))
	elif fecha != "":
            return self.conexion.ejecutarSQL("""
            select distinct(c.id), p.nombre, c.fecha, c.hora
            from Compras c, Proveedores p
            where c.id_Proveedor = p.id and c.fecha between '%s' and '%s'
            order by c.fecha"""%(fecha[0],fecha[1]))
    
    def guardarCompra(self, Colaborador, ProveedorCompra, totalCompra, Productos, mod):
        #print "JP:: guardarCompra ha iniciado..."
	if mod:
	    try:
		idCompra = Productos[0]
		modificados = Productos[1]
		eliminados = Productos[2]
		kardex = Productos[3]
		self.conexion.ejecutarSQL("""update Compras set fecha = DATE('now','localtime') \
		                             where id = %s"""%(idCompra))
		self.conexion.ejecutarSQL("""update Compras set hora = TIME('now','localtime') \
		                             where id = %s"""%(idCompra))
		self.conexion.ejecutarSQL("""update Compras set total = %s \
		                             where id = %s"""%(totalCompra,idCompra))
		self.conexion.ejecutarSQL("""update Compras set id_proveedor = %s \
		                             where id = %s"""%(ProveedorCompra,idCompra))

		#Ahora se actualiza los datos pertinentes en la tabla ProductoXCompras.
		
		#modificados: [numero_item,idProducto, id_Compra, cantidad, Total]
		for i in modificados:
		    try:
			self.conexion.ejecutarSQL("update ProductosXCompras set cantidad=%s \
			                          where numero_item=%s and codigo_producto='%s' and id_compra=%s"%(i[3],i[0],i[1],i[2]))
			self.conexion.ejecutarSQL("update ProductosXCompras set valor_total=%s \
			                          where numero_item=%s and codigo_producto='%s' and id_compra=%s"%(i[4],i[0],i[1],i[2]))
		    except Exception, e:
			print "GuardarCompra --> Guardar en ProductosXCompras: ", e
			self.conexion.rollback()
			return False
		
		# por 'ultimo se elimina los productos que se eliminaron de la compra.
		# eliminados: [numero_item, idProducto, id_Compra]
		for i in eliminados:
		    try:
			self.conexion.ejecutarSQL("delete from ProductosXCompras  \
			                          where numero_item=%s and  codigo_producto='%s' and id_compra=%s "%(i[0],i[1],i[2]))
		    except Exception, e:
			print "GuardarCompra --> Eliminar producto de una compra", e
			self.conexion.rollback()
			return False
		#Se actualiza el kardex dado que se hizo una modificación en la compra.
    
		# se recogen los datos necesarios.
		try:
		    for i in kardex:
			#i --- [codigo_producto, vUnitario, cantVieja, cantNueva]
			codigo_producto = int(i[0])
			vUnitario = float(i[1])
			cantVieja = float(i[2])
			cantNueva = float(i[3])
			print codigo_producto
			info = self.getKardexProducto(codigo_producto)
			#info --> saldo_valor, saldo_cantidad, costo_unitario, valor_total, detalle
			info = info[-1]
			
			if cantVieja > cantNueva:# es porque hubo una devolución en la compra
			    detalle = 'Venta'
			    cantidad = cantVieja - cantNueva
			    valorTotal = cantidad * vUnitario
			    sCantidad = float(info[1]) - cantidad
			    sValor = float(info[0]) - valorTotal    
			elif cantNueva > cantVieja:# es porque hubo una compra adicional
			    detalle = 'Compra'
			    cantidad = cantNueva - cantVieja
			    #vUnitario = float(info[2])
			    valorTotal = cantidad * vUnitario
			    sCantidad = float(info[1]) + cantidad
			    sValor = float(info[0]) + valorTotal
			else: # no hubo cambio, por lo que se deja como estaba
			    continue
			costo_unitario = sValor/float(sCantidad)
			self.conexion.ejecutarSQL("""
			    insert into Kardex (codigo_Producto, fecha, hora, detalle,cantidad, valor_total,saldo_cantidad, 
			                        saldo_valor, valor_unitario,costo_unitario) values 
			                ('%s',DATE('now','localtime'),TIME('now','localtime'),'%s',%s,%s,%s,%s,%s,%s )"""%(codigo_producto,
			                                                        detalle,cantidad,valorTotal,sCantidad,
			                                                        sValor,vUnitario,costo_unitario))
		except Exception, e:
		    print "GuardarCompra --> Actualizar kardex", e
		    self.conexion.rollback()
		    return False
		
		self.conexion.commit()
		return True
	    except Exception, e:
		print "guardarCompra excepcion: ", e
		self.conexion.rollback()
		return False
	else:
            #print "JP:: modo insercion nueva compra"
	    try:
		self.conexion.ejecutarSQL("insert into Compras (fecha, hora, total, id_Proveedor, usuario_Colaborador) values \
		                              (DATE('now','localtime'),TIME('now','localtime'),%s,%s,'%s')"%(totalCompra,ProveedorCompra,Colaborador))
		#Ahora se inserta los datos pertinentes en la tabla ProductoXCompras.
		idCompra = self.getIdUltimaCompra()
		if idCompra == -1:
                    print "GuardarCompra --> error con idCompra"
		    self.conexion.rollback()
		    return False
		#Productos [numero_item, idProducto, valorUnidad, cantidad, Total]
		for i in Productos:
		    try:
			self.conexion.ejecutarSQL("insert into ProductosXCompras values \
			                      (%s,'%s',%s,%s,%s,%s )"%(i[0],i[1],idCompra,i[3],i[2],i[4]))
		    except Exception, e:
			print "GuardarCompra --> Guardar en ProductosXCompras: ", e
			self.conexion.rollback()
			return False
		# Es una nueva compra. Se debe actualizar el inventario (kardex).
		for i in Productos:
		    cantidadKardex = i[3]
		    idProductoKardex = i[1]
		    v_unitarioKardex = i[2]
		    valor_TotalKardex = float(i[3])*float(i[2])
		    saldos = self.conexion.ejecutarSQL("""select saldo_cantidad, saldo_valor from kardex
                                                        where codigo_Producto='%s'
                                                        order by fecha and hora"""%(idProductoKardex))
		    if len(saldos) == 0:
			saldo_cantidadKardex = i[3]
			saldo_valorKardex = valor_TotalKardex
		    else:
			saldo_cantidadKardex = saldos[len(saldos)-1][0]+float(i[3])
			saldo_valorKardex = saldos[len(saldos)-1][1]+valor_TotalKardex
		    if float(saldo_cantidadKardex) == 0.0:
                        costo_unitarioKardex = saldo_valorKardex
                    else:
                        costo_unitarioKardex = saldo_valorKardex/float(saldo_cantidadKardex)
		    try:
			self.conexion.ejecutarSQL("""insert into Kardex (codigo_Producto, fecha, hora, detalle,cantidad, valor_total,saldo_cantidad, saldo_valor, valor_unitario,costo_unitario)
                                                    values ('%s',DATE('now','localtime'),TIME('now','localtime'),'Compra',%s,%s,%s,%s,%s,%s )"""
                                                  %(idProductoKardex, cantidadKardex, valor_TotalKardex, saldo_cantidadKardex,
                                                    saldo_valorKardex, v_unitarioKardex, costo_unitarioKardex))
		    except Exception, e:
			print "Kardex Compra: ", e
			self.conexion.rollback()
			return False
		self.conexion.commit()
		#print "JP:: true"
		return True
	    except Exception, e:
		print "guardarCompra excepcion: ", e
		self.conexion.rollback()
		return False
    
    def getProductosCompra(self,idCompra):
	return self.conexion.ejecutarSQL("""
            select p.numero_item, p.codigo_producto, p.valor_unitario, p.cantidad, p.valor_total, pro.nombre \
            from ProductosXCompras p, Compras c, Proveedores pro \
            where id_compra=%s and p.id_compra=c.id and c.id_proveedor=pro.id\
            order by valor_total"""%(idCompra))
    
    def ElimiarCompra(self, idCompra):
	try:
	    self.conexion.ejecutarSQL("delete from Compras where id=%s"%(idCompra))
	    #Ahora se elimina los productos relacionados con esa compra en la tabla ProductoXCompras.
	    self.conexion.ejecutarSQL("delete from ProductosXCompras where id_compra=%s"%(idCompra))
	    self.conexion.commit()
	    return True
	except Exception, e:
	    print "EliminarCompra excepcion: ", e
	    self.conexion.rollback()
	    return False
    

    #################Admin Ventas#########################################


    def getVentasXcolaboradores(self, fechaInicio="", fechaFin=""):
        if fechaInicio != "" and fechaFin != "":
            return self.conexion.ejecutarSQL("""
            select usuario_colaborador, sum(total)
            from ventas
            where fecha between '%s' and '%s'
            group by usuario_colaborador"""%(fechaInicio,fechaFin))
        else:
            return self.conexion.ejecutarSQL("""
            select usuario_colaborador, sum(total)
            from ventas
            group by usuario_colaborador""")


    def getVentasXcliente(self, idCliente):
        """Retorna las ventas que se le han hecho a un cliente especifico"""
        return self.conexion.ejecutarSQL("""select v.id, v.fecha, v.total, v.estado, tp.tipo, v.usuario_colaborador, v.fechaPagoTotal, julianday(v.fechaPagoTotal) - julianday(v.fecha)
                                            from ventas v, tipoPagos tp
                                            where v.id_tipoPago = tp.id
                                            and v.id_Cliente = '%s'
                                            order by v.id desc"""%(idCliente))

	
    def getVentasAdmin(self, fechaInicio="", fechaFin="", idFactura="", idCliente="", tipoPago="", estado="", idProducto=""):
	if fechaInicio == "" and fechaFin == "" and idFactura == "" and idCliente == "" and tipoPago == "" and estado == "":
	    return self.conexion.ejecutarSQL("""
            select v.id, v.fecha, v.hora, v.subtotal, v.totalIVA, v.total, v.estado, co.usuario, v.id_tipoPago, v.id_cliente, c.nombres || ' ' || c.primer_apellido
            from Colaboradores co, Ventas v left outer join Clientes c on v.id_cliente = c.id
            where v.usuario_colaborador = co.usuario""")
	elif fechaInicio != "" and fechaFin != "":
            return self.conexion.ejecutarSQL("""
            select v.id, v.fecha, v.hora, v.subtotal, v.totalIVA, v.total, v.estado, co.usuario, v.id_tipoPago, v.id_cliente, c.nombres || ' ' || c.primer_apellido
            from Colaboradores co, Ventas v left outer join Clientes c on v.id_cliente = c.id
            where v.usuario_colaborador = co.usuario
            and v.fecha between '%s' and '%s'"""%(fechaInicio,fechaFin))
        
##	elif idProducto != "":
##	    return self.conexion.ejecutarSQL("""
##            select distinct(v.id), v.estado, v.total, co.usuario, cl.id\
##            from Ventas v, Colaboradores co, Clientes cl, Productos p, ProductosXVentas pv \
##            where v.id_cliente=cl.id and v.usuario_colaborador=co.usuario and pv.id_venta=v.id \
##	    and pv.codigo_producto=p.codigo and p.codigo=%s"""%(idProducto))
##	elif rangoFecha != "":
##	    return self.conexion.ejecutarSQL("""
##            select v.id, v.estado, v.total, co.usuario, cl.id\
##            from Ventas v, Colaboradores co, Clientes cl \
##            where v.id_cliente=cl.id and v.usuario_colaborador=co.usuario and\
##	    v.fecha between '%s' and '%s'"""%(rangoFecha[0],rangoFecha[1]))
##	if idFactura != "":
##	    return self.conexion.ejecutarSQL("""
##            select v.id, v.estado, v.total, co.usuario, cl.id\
##            from Ventas v, Colaboradores co, Clientes cl \
##            where v.id_cliente=cl.id and v.usuario_colaborador=co.usuario\
##	    and v.id=%s"""%(idFactura))
##	elif idCliente != "":
##	    return self.conexion.ejecutarSQL("""
##            select v.id, v.estado, v.total, co.usuario, cl.id\
##            from Ventas v, Colaboradores co, Clientes cl \
##            where v.id_cliente=cl.id and v.usuario_colaborador=co.usuario\
##	    and cl.id=%s"""%(idCliente))
##	elif tipoPago != "":
##	    return self.conexion.ejecutarSQL("""
##            select v.id, v.estado, v.total, co.usuario, cl.id\
##            from Ventas v, Colaboradores co, Clientes cl, TipoPagos tp\
##            where v.id_cliente=cl.id and v.usuario_colaborador=co.usuario\
##	    and v.id_tipopago = tp.id and tp.id = %s"""%(tipoPago))
##	elif estado != "":
##	    return self.conexion.ejecutarSQL("""
##            select v.id, v.estado, v.total, co.usuario, cl.id\
##            from Ventas v, Colaboradores co, Clientes cl \
##            where v.id_cliente=cl.id and v.usuario_colaborador=co.usuario\
##	    and v.estado='%s'"""%(estado))
	
    def getProductosVenta(self,idVenta):
        """Retorna todos los productos (items) asociados al id de venta dado."""
	return self.conexion.ejecutarSQL("""
	    select pv.numero_item, pv.codigo_producto,v.fecha, pv.valor_unitario, pv.cantidad, pv.iva, pv.valor_total 
	    from ProductosXVentas pv, Ventas v 
	    where v.id = pv.id_venta and pv.id_venta=%s"""%(idVenta))


    def getItemVenta(self, idVenta, codigo):
        """Retorna un (1) item de venta de la venta dada que tenga el codigo dado."""
        return self.conexion.ejecutarSQL("""
                select numero_item, codigo_producto, id_venta, usuario_colaborador, cantidad, valor_unitario, IVA, valor_total
                from productosXventas
                where id_venta = %s and codigo_producto = '%s'
                limit 1"""%(idVenta, codigo))


    def eliminarItemVenta(self, numeroItem, codigo, idVenta):
        """Elimina el item con codigo dado en la venta dada."""
        try:
            self.conexion.ejecutarSQL("""delete from productosXventas
                                        where numero_item = %s and codigo_producto = '%s' and id_venta = %s"""%(numeroItem, codigo, idVenta))
            self.conexion.commit()
            return True
        except Exception, e:
            print "eliminarItemVenta --> ", e
            self.conexion.rollback()
            return False


    def insertarItemVenta(self, numeroItem, codigo, idVenta, vendedor, cantidad, valorUnit, IVA, total):
        """Inserta el item dado a la venta indicada."""
        try:
            self.conexion.ejecutarSQL("""insert into productosXventas values (%s, '%s', %s, '%s', %s, %s, %s, %s)"""
                                      %(numeroItem, codigo, idVenta, vendedor, cantidad, valorUnit, IVA, total))
            self.conexion.commit()
            return True
        except Exception, e:
            print "insertarItemVenta --> ", e
            self.conexion.rollback()
            return False
    
    
    def getTipoPagos(self):
	return self.conexion.ejecutarSQL("""
            select id, tipo from TipoPagos""")
    
    def modificarProductosVenta(self, listaProductos):
	"""Modifica un producto de una venta dado el id de la venta, el codigo del producto y el numero_item, se puede modificar la cantidad de productos
	en la venta o eliminarlo"""
	#modificar -- [idventa, codProducto, numeroitem, valorUnitario, cantidad, IVA]
	modificar = listaProductos[0]
	#eliminar -- [idventa, idProducto, numeroitem]
	eliminar = listaProductos[1]
	# primero se modifican los productos
	for i in modificar:
	    try:
                self.conexion.ejecutarSQL("""update ProductosXVentas
                                                set valor_unitario=%s,
                                                    cantidad=%s,
                                                    IVA=%s,
                                                    valor_total=%s*%s
                                                where id_Venta=%s
                                                    and codigo_Producto = '%s'
                                                    and numero_item=%s"""%(i[3], i[4], i[5], i[4], i[3], i[0], i[1], i[2]))
	    except Exception, e:
		print "modificarProductosVenta excepcion: ", e
		self.conexion.rollback()
		return False
	# Ahora se eliminan los Productos
	for i in eliminar:
	    try:
		self.conexion.ejecutarSQL("delete from ProductosXVentas where id_Venta=%s and codigo_Producto = '%s' and numero_item=%s"%(i[0],i[1],i[2]))
	    except Exception, e:
		print "modificarProductosVenta --> Eliminar excepcion: ", e
		self.conexion.rollback()
		return False
	    
	#Se actualiza el kardex para las ventas modificadas como para las ventas eliminadas.
	
	#Para las ventas modificadas
	# PENDIENTE REVISAR SI HUBO CAMBIO DE VALOR UNITARIO
	try:
	    #modificar -- [idventa,idProducto,numeroitem,cantidad,cantidad_anterior]
	    for i in modificar:
		cantActual = float(i[3])
		cantAnterior = float(i[4])
		if cantActual == cantAnterior: #S'olo se actualiza el kardex si hubo cambio
		    continue
		else:
		    codigoProducto = i[1]
		    info = self.getKardexProducto(codigoProducto)
		    #info --> saldo_valor, saldo_cantidad, costo_unitario, valor_total, detalle
		    info = info[-1]
		    if cantActual < cantAnterior: # Si hizo una devoluci'on en la venta (el cliente me devolvi'o cant productos)
			cant = cantAnterior - cantActual
			detalle = 'Compra'
			valorUnitario = float(info[2])
			valorTotal = valorUnitario*cant
			saldoCantidad = float(info[1])+cant
			saldoValor = float(info[0])+valorTotal
		    else:# Si hizo una adici'on a la venta (le tengo que vender cant productos al cliente)
			cant = cantActual - cantAnterior
			detalle = 'Venta'
			valorUnitario = float(info[2])
			valorTotal = valorUnitario*cant
			saldoCantidad = float(info[1])-cant
			saldoValor = float(info[0])-valorTotal
		    costoUnitario = saldoValor/float(saldoCantidad)
		    self.conexion.ejecutarSQL("""
		    insert into Kardex (codigo_Producto, fecha, hora, detalle,cantidad, valor_total,saldo_cantidad, 
		    saldo_valor, valor_unitario,costo_unitario) values 
		    ('%s',DATE('now','localtime'),TIME('now','localtime'),'%s',%s,%s,%s,%s,%s,%s )"""%(codigoProducto,
		                                                                                     detalle,cant,valorTotal,saldoCantidad,
		                                                                                     saldoValor,valorUnitario,costoUnitario))
	    self.conexion.commit()
	    return True
	except:
	    print "modificarProductosVenta --> Actualizar kardex (ventas modificadas) excepcion: ", e
	    self.conexion.rollback()
	    return False


    
    def eliminarVentaAdmin(self,idVenta,lstids):
	#Para eliminar una venta, se debe primero eliminar los productos de esa venta
	# en la tabla ProductosXVentas.
	# PENDIENTE - Borraria la venta aun si tiene abonos. Que hacer en este caso? --> Avisar que se borraran tambien los abonos
	try:
	    for i in lstids:
		try:
		    self.conexion.ejecutarSQL("delete from ProductosXVentas where numero_item=%s\
		                               and id_venta=%s and codigo_producto='%s'"%(i[0],idVenta,i[1]))
		except Exception, e:
		    print "Eliminar Producto en ProductosXCompras: ", e
		    self.conexion.rollback()
		    return False
	    self.conexion.ejecutarSQL("delete from Ventas where id=%s"%(idVenta))
	    self.conexion.commit()
	    return True
	except Exception, e:
	    print "EliminarVentaAdmin excepcion: ", e
	    self.conexion.rollback()
	    return False
	
	
    def getProductos_IngresarCompra(self, codigo):
        if codigo != "":
            return self.conexion.ejecutarSQL("select precio_venta \
                                             from productos \
                                             where codigo = '%s'"%(codigo))
    def getIdClientes(self):
	return self.conexion.ejecutarSQL("select id from Clientes")
    
    def getIdProveedores(self):
	return self.conexion.ejecutarSQL("select id from Proveedores")
    
    def activarCliente(self,id):
	try:
	    self.conexion.ejecutarSQL("update Clientes set activo='SI' where id=%s"%(id))
	    self.conexion.commit()
	except:
	    self.conexion.rollback()


    def actualizarTotalVenta(self, idVenta, subtotal, totalIVA, totalVenta):
	try:
	    self.conexion.ejecutarSQL("update Ventas set subtotal=%s, totalIVA=%s, total=%s where id=%s"%(subtotal, totalIVA, totalVenta, idVenta))
	    self.conexion.commit()
	    return True
	except Exception, e:
            print "actualizarTotalVenta excepcion: ", e
	    self.conexion.rollback()
	    return False


    def getKardexGeneral(self):
	return self.conexion.ejecutarSQL("""select codigo_Producto, saldo_valor, saldo_cantidad, costo_unitario, valor_total, detalle
                                            from Kardex
                                            order by codigo_Producto and fecha and hora""")
    
    def getKardexProducto(self,idProducto):
	return self.conexion.ejecutarSQL("""select saldo_valor, saldo_cantidad, costo_unitario, valor_total, detalle
                                            from Kardex
                                            where codigo_Producto = '%s'
                                            order by fecha and hora"""%(idProducto))
    
os.chdir(os.getcwd())
# leer ip y puerto de sc.conf
conf = open("sc.conf")
l = conf.readlines()
ip, puerto = l[0][:-1], int(l[1][:-1])
conf.close()
# definicion del servidor y puesta en marcha
s = SimpleXMLRPCServer((ip, puerto), allow_none=True)
s.register_introspection_functions()
s.register_instance(servidorCentral())
s.serve_forever()
