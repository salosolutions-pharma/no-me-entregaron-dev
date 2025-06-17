#!/bin/bash
# Script para migrar cambios estables de DEV a MAIN

set -e

echo "🌿 Migración DEV → MAIN"
echo "======================="

# Colores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Función para logging con colores
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Verificar que estamos en un repo git
if [[ ! -d ".git" ]]; then
    log_error "No estás en un repositorio Git"
    exit 1
fi

# Obtener rama actual
current_branch=$(git branch --show-current)
log_info "Rama actual: $current_branch"

# Verificar estado limpio
if [[ -n $(git status --porcelain) ]]; then
    log_error "Tienes cambios sin commitear. Hazlo primero:"
    git status --short
    exit 1
fi

# Actualizar referencias
log_info "Actualizando referencias remotas..."
git fetch origin

# Función para mostrar commits pendientes
show_pending_commits() {
    local from_branch=$1
    local to_branch=$2
    
    echo ""
    log_info "Commits en $from_branch que NO están en $to_branch:"
    echo "=================================================="
    
    if git log origin/$to_branch..origin/$from_branch --oneline | head -20; then
        echo ""
        local count=$(git log origin/$to_branch..origin/$from_branch --oneline | wc -l)
        log_info "Total de commits pendientes: $count"
    else
        log_warn "No hay commits pendientes de $from_branch a $to_branch"
        return 1
    fi
}

# Función para merge completo
do_full_merge() {
    log_info "Iniciando merge completo DEV → MAIN"
    
    # Ir a main
    git checkout main
    git pull origin main
    
    # Merge desde dev
    log_info "Mergeando dev en main..."
    if git merge origin/dev --no-edit; then
        log_info "✅ Merge exitoso"
        
        # Mostrar resumen
        echo ""
        log_info "📋 Resumen de cambios aplicados:"
        git log --oneline -10
        
        # Preguntar si subir
        echo ""
        read -p "🚀 ¿Subir cambios a main? (y/N): " -n 1 -r
        echo ""
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            git push origin main
            log_info "✅ Cambios subidos a main"
        else
            log_warn "Cambios listos localmente. Para subirlos: git push origin main"
        fi
    else
        log_error "❌ Conflictos detectados"
        log_info "Resuelve manualmente y ejecuta:"
        log_info "  git add ."
        log_info "  git commit"
        log_info "  git push origin main"
        exit 1
    fi
}

# Función para cherry-pick selectivo
do_cherry_pick() {
    log_info "Modo cherry-pick selectivo"
    
    # Mostrar commits disponibles
    show_pending_commits "dev" "main"
    
    echo ""
    log_info "Ingresa los hash de commits que quieres aplicar a main"
    log_info "Ejemplo: abc1234 def5678 ghi9012"
    echo ""
    
    read -p "Commits a cherry-pick: " commits
    
    if [[ -z $commits ]]; then
        log_error "No ingresaste ningún commit"
        exit 1
    fi
    
    # Ir a main
    git checkout main
    git pull origin main
    
    # Aplicar commits
    log_info "Aplicando commits: $commits"
    for commit in $commits; do
        log_info "📥 Aplicando: $commit"
        if git cherry-pick $commit; then
            log_info "✅ Commit $commit aplicado"
        else
            log_error "❌ Conflicto en $commit"
            log_info "Resuelve y continúa con: git cherry-pick --continue"
            exit 1
        fi
    done
    
    # Preguntar si subir
    echo ""
    read -p "🚀 ¿Subir cambios a main? (y/N): " -n 1 -r
    echo ""
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git push origin main
        log_info "✅ Cherry-pick completado y subido"
    else
        log_warn "Cambios listos localmente"
    fi
}

# Función para crear Pull Request
create_pull_request() {
    log_info "Creando Pull Request DEV → MAIN"
    
    if ! command -v gh >/dev/null 2>&1; then
        log_error "GitHub CLI no está instalado"
        log_info "Instálalo con: https://cli.github.com/"
        log_info "O crea el PR manualmente en GitHub"
        exit 1
    fi
    
    # Mostrar commits pendientes
    show_pending_commits "dev" "main"
    
    echo ""
    read -p "¿Crear PR con estos cambios? (y/N): " -n 1 -r
    echo ""
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        gh pr create \
            --title "🚀 Migración DEV → MAIN - $(date '+%Y-%m-%d')" \
            --body "## 📋 Cambios incluidos:
$(git log origin/main..origin/dev --oneline)

## ✅ Checklist:
- [ ] Código revisado y probado
- [ ] Tests pasando
- [ ] Documentación actualizada
- [ ] Variables de entorno verificadas
- [ ] Sin breaking changes

## 🎯 Tipo de release:
- [ ] 🐛 Bug fix
- [ ] ✨ Nueva funcionalidad  
- [ ] 🔧 Mejora/optimización
- [ ] 📚 Solo documentación

**Listo para merge a MAIN (ambiente estable)**" \
            --base main \
            --head dev
        
        log_info "✅ Pull Request creado"
        log_info "Revisa y aprueba en GitHub para completar la migración"
    else
        log_warn "PR cancelado"
    fi
}

# Menú principal
echo ""
log_info "¿Cómo quieres migrar los cambios de DEV a MAIN?"
echo "1) 📋 Ver qué cambios hay pendientes"
echo "2) 🔀 Merge completo (todo lo de dev)"
echo "3) 🍒 Cherry-pick selectivo (commits específicos)"
echo "4) 📝 Crear Pull Request (recomendado)"
echo "5) 🚪 Salir"
echo ""

read -p "Selecciona una opción (1-5): " choice

case $choice in
    1)
        show_pending_commits "dev" "main"
        ;;
    2)
        # Verificar que hay cambios
        if show_pending_commits "dev" "main"; then
            echo ""
            read -p "¿Continuar con merge completo? (y/N): " -n 1 -r
            echo ""
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                do_full_merge
            else
                log_warn "Merge cancelado"
            fi
        fi
        ;;
    3)
        if show_pending_commits "dev" "main"; then
            do_cherry_pick
        fi
        ;;
    4)
        if show_pending_commits "dev" "main"; then
            create_pull_request
        fi
        ;;
    5)
        log_info "👋 ¡Hasta luego!"
        exit 0
        ;;
    *)
        log_error "Opción inválida"
        exit 1
        ;;
esac

# Volver a dev para continuar trabajando
if [[ $(git branch --show-current) != "dev" ]]; then
    log_info "🔄 Volviendo a rama dev para continuar desarrollo..."
    git checkout dev
fi

echo ""
log_info "🎉 Operación completada!"
log_info "📍 Rama actual: $(git branch --show-current)"