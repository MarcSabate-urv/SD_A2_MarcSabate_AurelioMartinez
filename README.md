# SD_A2_MarcSabate_AurelioMartinez

El objetivo de este código es proporcionar una implementación distribuida de un algoritmo de exclusión mutua utilizando IBM COS. Habrá
una función master y un número variable de funciones slave. Todas las funciones slave actualizarán un objeto compartido común llamado
"Result.json", agregando en este archivo la identificación de la función slave cuando obtiene permiso para escribir.
