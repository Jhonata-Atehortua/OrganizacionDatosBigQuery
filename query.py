from google.cloud import bigquery
from google.oauth2 import service_account
import time
import json
import pandas

#Id del proyecto
id_proyecto = "entrenamiento-gcp-267213" 

#Path de donde se encuentran los credenciales
credenciales = service_account.Credentials.from_service_account_file(r"C:\Users\Jhonatan\Documents\BigQuery\entrenamiento-gcp-267213-1da7a0e41454.json") 

#Realizar una conexion con el cliente de bigquery
cliente = bigquery.Client(project=id_proyecto,credentials=credenciales)

#Tomar el tiempo en el que inicio el proceso y enviar un mensaje
Inicio = time.time()
print("Inicio Proceso 👏")

#Consulta realizada con los datos necesarios para trabajar
query = """ 
        SELECT
            id_cia,id_co,instalacion,id_bodega,CAST(fecha AS STRING) as fecha,CAST(hora AS STRING) as hora ,ubicacion,referencia,descripcion,lote,unidad_medida,valor_inventario,usuario,valor_equivalente,cantidad_total,medida_caj,cantidad_caja,equiv_caja,vacio,nro_tarjeta,cajas_estiba,estiba_completa,tipo_medida,unidad_estiba,conteo,
            CONCAT(id_cia,"-",id_co,"-",instalacion,"-",id_bodega,"-",fecha,"-",ubicacion,"-",referencia,"-",nro_tarjeta) AS llave1,
            CONCAT(id_cia,"-",id_co,"-",instalacion,"-",id_bodega,"-",fecha,"-",ubicacion,"-",referencia,"-",nro_tarjeta,"-",cantidad_total) AS llave2,
            ROW_NUMBER() OVER (PARTITION BY id_cia, id_co, instalacion, id_bodega, fecha, ubicacion,referencia, nro_tarjeta ORDER BY conteo) AS row_num,
            ROW_NUMBER() OVER (PARTITION BY id_cia, id_co, instalacion, id_bodega, fecha, ubicacion,referencia, nro_tarjeta,cantidad_total ORDER BY conteo) AS row_num2
            FROM
            `entrenamiento-gcp-267213.training.conteos2` order by id_cia, id_co, instalacion, id_bodega, fecha, ubicacion,referencia, nro_tarjeta, conteo LIMIT 10
        """

#Ejecucion del query
query_execution0 = cliente.query(query) 

#Obtener el resultado del query y almacenarlo como un dataframe para que el proceso sea mas rapido
ResultadoC = query_execution0.result().to_dataframe()

#Obtener los datos unicos de la columna llave1 (para que solo verifique con una llave)
LlavesUnicas = ResultadoC.drop_duplicates(subset='llave1')

#Variable donde se va almacenar los datos del proceso
Datos = []

#Iterar sobre cada una de las llaves unicas
for x in range(0,len(LlavesUnicas)):
    # print(LlavesUnicas.iloc[x]['llave1'])
    #Almacenar llave1
    llave = LlavesUnicas.iloc[x]['llave1']

    #Almacenar llave2
    llave2 = LlavesUnicas.iloc[x]['llave2']

    #Se definen las variables para el proceso
    Observacion = ""
    Organizacion = {}
    Conteo =1
    IngresoD = False
    CantidadCI = 0

    #Verificar si con la llave1 existe un solo registro sin importar que tenga un conteo alto
    if len(ResultadoC[ResultadoC['llave1']==llave]) == 1:
        #Mensaje de obeservacion
        Observacion = "SE REQUIERE PRIMER O SEGUNDO CONTEO"
    else:
        #Verificar si existe mas de un registro con las misma llave2 (la llave2 contiene la cnatidad total si hay mas de 1 quiere decir que debe ir OK)
        LlavesUnicas2 = ResultadoC.loc[ResultadoC['llave1']==llave]['llave2'].unique()
        
        for z in range(0,len(LlavesUnicas2)):
            ResultadoLlaves2 = ResultadoC[ResultadoC['llave2'] == LlavesUnicas2[z]]
            if len(ResultadoLlaves2) > 1:
                CantidadCI = len(ResultadoLlaves2)

        if CantidadCI > 1:
            #Mensaje de obeservacion
            Observacion = "OK"
        else:
            #Varifica cual es el conteo mas alto que contenga la llave para saber que observacion debe poner
            ConteoMAL = ResultadoC.loc[ResultadoC['llave1'] == llave,'conteo'].max()
            #Si es 2 quiere decir que necesita un 3 conteo
            if ConteoMAL == 2:
                Observacion = "SE REQUIERE TERCER CONTEO"
            elif ConteoMAL == 3: #Si es 3 quiere decir que necesita un 4 conteo
                Observacion = "SE REQUIERE CUARTO CONTEO"
    
    #Obtener todos los resultados que encuentre en el dataframe con esa llave1
    ResultadoLlaveU = ResultadoC[ResultadoC['llave1']==llave]

    #Recorrer la cantidad de resulados con la llave1
    for o in range(0,len(ResultadoLlaveU)):
        # print(ResultadoLlaveU)
        #Verificar que no se hallan ingresado datos todavia ese solo funciona una sola vez 
        if IngresoD == False:
            #Obtener el conteo mas alto o del ultimo
            ConteoM = ResultadoLlaveU.loc[ResultadoLlaveU['llave1'] == llave,'conteo'].max()

            #Obtener los datos del ultimo conteo mas alto
            DatosUltimoC = ResultadoLlaveU[(ResultadoLlaveU['llave1'] == llave) & (ResultadoLlaveU['conteo'] == ConteoM)]

            #Estructura de los datos del contador mas alto
            Organizacion['id_cia']= int(DatosUltimoC.iloc[0]['id_cia'])
            Organizacion['id_co']= int(DatosUltimoC.iloc[0]['id_co'])
            Organizacion['instalacion']= int(DatosUltimoC.iloc[0]['instalacion'])
            Organizacion['id_bodega']= DatosUltimoC.iloc[0]['id_bodega']
            Organizacion['fecha'] = DatosUltimoC.iloc[0]['fecha']
            Organizacion['hora']= DatosUltimoC.iloc[0]['hora']
            Organizacion['ubicacion']= DatosUltimoC.iloc[0]['ubicacion']
            Organizacion['referencia']= DatosUltimoC.iloc[0]['referencia']
            Organizacion['descripcion']= DatosUltimoC.iloc[0]['descripcion']
            Organizacion['lote']= int(DatosUltimoC.iloc[0]['lote'])
            Organizacion['unidad_medida']= DatosUltimoC.iloc[0]['unidad_medida']
            Organizacion['valor_inventario']= int(DatosUltimoC.iloc[0]['valor_inventario'])
            Organizacion['usuario']= DatosUltimoC.iloc[0]['usuario']
            Organizacion['valor_equivalente']= int(DatosUltimoC.iloc[0]['valor_equivalente'])
            Organizacion['cantidad_total']= int(DatosUltimoC.iloc[0]['cantidad_total'])
            Organizacion['medida_caj']= DatosUltimoC.iloc[0]['medida_caj']
            Organizacion['cantidad_caja']= int(DatosUltimoC.iloc[0]['cantidad_caja'])
            Organizacion['equiv_caja']= int(DatosUltimoC.iloc[0]['equiv_caja'])
            Organizacion['vacio']= DatosUltimoC.iloc[0]['vacio']
            Organizacion['nro_tarjeta']= int(DatosUltimoC.iloc[0]['nro_tarjeta'])
            Organizacion['cajas_estiba']= int(DatosUltimoC.iloc[0]['cajas_estiba'])
            Organizacion['estiba_completa']= DatosUltimoC.iloc[0]['estiba_completa']
            Organizacion['tipo_medida']= DatosUltimoC.iloc[0]['tipo_medida']
            Organizacion['unidad_estiba']= DatosUltimoC.iloc[0]['unidad_estiba']
            Organizacion['conteo']= int(DatosUltimoC.iloc[0]['conteo'])
            Organizacion['observaciones']= Observacion
            IngresoD = True 
        
        #Estructura de los datos despues del contador mas alto (Todos los datos en orden)
        Organizacion[f'valor_inventario{Conteo}']=int(ResultadoLlaveU.iloc[o]['valor_inventario'])
        Organizacion[f'cantidad_caja{Conteo}']=int(ResultadoLlaveU.iloc[o]['cantidad_caja'])
        Organizacion[f'cantidad_total{Conteo}']=int(ResultadoLlaveU.iloc[o]['cantidad_total'])
        Organizacion[f'usuario{Conteo}']=ResultadoLlaveU.iloc[o]['usuario']
        Organizacion[f'item_conteo{Conteo}']=ResultadoLlaveU.iloc[o]['referencia']
        
        
        Conteo = Conteo+1

        #Validad si ya llego a la ultima vuelta
        if o == len(ResultadoLlaveU)-1:
            #Verificar si no estuvieron los 4 conteos completos 
            if Conteo != 5:
                #Definir datos vacios si faltaron conteos 
                for c in range(Conteo,5):
                    Organizacion[f'valor_inventario{c}']=0
                    Organizacion[f'cantidad_caja{c}']=0
                    Organizacion[f'cantidad_total{c}']=0
                    Organizacion[f'usuario{c}']=""
                    Organizacion[f'item_conteo{c}']=""
            #Añadir los datos a un lugar para luego trabajarlos
            Datos.append(Organizacion)

with open("Datos.json",'w') as archivo:
    for dato in Datos:
        json.dump(dato,archivo)
        archivo.write('\n')

#Toma el tiempo final del proceso y envia el mensaje de finalizacion y el tiempo que se demoro ejecutando es en segundos
Final = time.time()
print("Proceso Finalizado 😊")   
print("Tiempo Ejecucion: ",Final-Inicio)       
