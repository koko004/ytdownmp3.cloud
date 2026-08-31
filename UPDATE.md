# Actualizar ytdownmp3 en el Host de Producción

## Opción 1: Usando imágenes pre-construidas de Docker Hub (Recomendado)

```bash
# 1. Ir al directorio del proyecto
cd /ruta/donde/tengas/docker-compose.yml

# 2. Descargar las últimas imágenes
docker compose pull

# 3. Reiniciar servicios con las nuevas imágenes
docker compose up -d --force-recreate

# 4. Verificar que todo está corriendo
docker compose ps

# 5. Ver logs si hay problemas
docker compose logs -f
```

## Opción 2: Si tienes el código fuente local y quieres rebuild

```bash
# 1. Actualizar código desde GitHub
git pull origin main

# 2. Rebuild y restart
docker compose up -d --build --force-recreate
```

## Verificar que la actualización fue exitosa

```bash
# Comprobar versión de las imágenes
docker compose images

# Probar endpoints
curl -s http://localhost:3000/ | head -3
curl -s http://localhost:8080/health
curl -s http://localhost:4000/api/health

# Ver estado de servicios en admin panel
curl -s http://localhost:8080/public/progress-bar
```

## Servicios y puertos

| Servicio | Puerto | Descripción |
|----------|--------|-------------|
| Frontend | 3000 | App principal |
| API | 8080 | Backend API |
| Admin | 4000 | Panel admin (requiere login) |
| Redis | 6379 | Cola y cache |

## Notas importantes

- **Admin panel**: `admin` / `changeme123` (cambia con `ADMIN_USER`/`ADMIN_PASS` en docker-compose.yml)
- **Progress bar**: Se configura en Admin → Settings → guarda en Redis
- **Volúmenes persistentes**: `redis-data` y `downloads` no se borran al actualizar
