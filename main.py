#!/bin/env python
import pywren_ibm_cloud as pywren
import json
import time as t

# ambda para ordenar la lista
get_last_modified = lambda obj: int(obj['LastModified'].timestamp())

# inicializacion de variables
N_SLAVES = 5

BUCKET_NAME = 'depositoaurelio'
namew = 'write_'
namep = 'p_write_'
namej = 'Result.json'
x = 0.100
s = ''
data = []


def master(x, ibm_cos):
    write_permission_list = []
    # bucle hasta que se que acaben los esclavos
    while 1:
        updated = 0
        # Lista de los p_write por orden
        aux_list = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix=namep)
        try:
            #mientras queden
            aux_list = [obj['Key'] for obj in sorted(aux_list["Contents"], key=get_last_modified, reverse=True)]
        except:
            #cuando no quedan
            return write_permission_list

        # Coger el mas antiguo y su id
        aux2 = aux_list[0]
        id = aux2[8:]

        # Coger result.json antiguo
        json1 = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=namej)['Body'].read()
        json1 = json.loads(json1)

        # put write{id} y delete del p_write seleccionado
        ibm_cos.put_object(Bucket=BUCKET_NAME, Key=namew + id, Body=s)
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=aux2)

        # añadimos a la write_permission_list este id
        write_permission_list.append(id)

        # bucle con sleep hasta que el result.json se haya actualizado
        while updated != 1:
            json2 = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=namej)['Body'].read()
            json2 = json.loads(json2)
            if json2 != json1:
                updated = 1
            else:
                t.sleep(x)

        # borrar write{id}
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=namew + id)



def slave(id, x, ibm_cos):
    found = 0
    # string id
    a = '{'+str(id)+'}'
    # put del p_write{id}
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=namep + a, Body=s)

    # bucle hasta ver el write{id}
    while found != 1:

        try:
            ibm_cos.get_object(Bucket=BUCKET_NAME, Key=namew + a)
            found = 1
        except:
            t.sleep(x)
    # Coger result.json, añadir el {id}, subir result.json
    jason = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=namej)['Body'].read()
    data = json.loads(jason)
    data.append(a)
    jason = json.dumps(data)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=namej, Body=jason)


if __name__ == '__main__':

    # Función inicial del codigo
    pw = pywren.ibm_cf_executor()
    ibm_cos = pw.internal_storage.get_client()
    data = json.dumps([])

    # result.json vacio
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=namej, Body=data)
    
    # llamadas a las funciones
    pw.map(slave, range(N_SLAVES))
    pw.call_async(master, x)

    # coger resultados y comparar
    write_permission_list = pw.get_result()

    result = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=namej)['Body'].read()
    result = json.loads(result)
    if write_permission_list == result:
        print("Las listas son iguales.")
    else:
        print("Las listas son diferentes.")


    #bucle para borrar los archivos intermedios (pywren.jobs)
    print("Borrando los archivos intermedios... ")
    lista = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix='pywren.jobs')
    aux_list = [obj['Key'] for obj in lista["Contents"]]
    for a in aux_list:
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=a)

