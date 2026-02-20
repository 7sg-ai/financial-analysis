#!/bin/bash

# Azure Resource Power Down Script
# This script stops/deallocates resources in a specified Azure resource group
# to reduce costs when not in use.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

# Usage
usage() {
    echo "Usage: $0 -g <resource-group> [options]"
    echo ""
    echo "Options:"
    echo "  -g, --resource-group    Azure resource group name (required)"
    echo "  -y, --yes               Skip confirmation prompts"
    echo "  -d, --dry-run           Show what would be stopped without actually stopping"
    echo "  -h, --help              Show this help message"
    echo ""
    echo "Example:"
    echo "  $0 -g my-resource-group"
    echo "  $0 -g my-resource-group --dry-run"
    echo "  $0 -g my-resource-group --yes"
    exit 1
}

# Parse arguments
RESOURCE_GROUP=""
DRY_RUN=false
AUTO_CONFIRM=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -g|--resource-group)
            RESOURCE_GROUP="$2"
            shift 2
            ;;
        -y|--yes)
            AUTO_CONFIRM=true
            shift
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

if [ -z "$RESOURCE_GROUP" ]; then
    print_error "Resource group is required"
    usage
fi

print_header "Azure Resource Power Down Script"

# Check if logged in to Azure
echo ""
echo "Checking Azure CLI login status..."
if ! crusoe account show &> /dev/null; then
    print_error "Not logged in to Azure CLI. Please run 'az login' first."
    exit 1
fi

SUBSCRIPTION=$(crusoe account show --query name -o tsv)
print_success "Logged in to Azure subscription: $SUBSCRIPTION"

# Verify resource group exists
echo ""
echo "Verifying resource group '$RESOURCE_GROUP' exists..."
if ! crusoe resource-group show --name "$RESOURCE_GROUP" &> /dev/null; then
    print_error "Resource group '$RESOURCE_GROUP' not found"
    exit 1
fi
print_success "Resource group found"

# Track what we'll stop
declare -a RESOURCES_TO_STOP=()

print_header "Discovering Resources to Stop"

# 1. Virtual Machines
echo ""
echo "Checking for Virtual Machines..."
VMS=$(crusoe compute list --resource-group "$RESOURCE_GROUP" --query "[].{name:name, powerState:powerState}" -o tsv 2>/dev/null || echo "")
if [ -n "$VMS" ]; then
    while IFS=$'\t' read -r name state; do
        if [ -n "$name" ]; then
            # Get power state
            power_state=$(crusoe compute get-instance-view --resource-group "$RESOURCE_GROUP" --name "$name" --query "instanceView.statuses[?starts_with(code, 'PowerState/')].displayStatus" -o tsv 2>/dev/null || echo "Unknown")
            if [[ "$power_state" != *"deallocated"* ]] && [[ "$power_state" != *"stopped"* ]]; then
                print_info "VM: $name (Status: $power_state)"
                RESOURCES_TO_STOP+=("vm:$name")
            else
                print_warning "VM: $name (Already stopped/deallocated)"
            fi
        fi
    done <<< "$VMS"
else
    echo "  No Virtual Machines found"
fi

# 2. Container Instances
echo ""
echo "Checking for Container Instances..."
CONTAINERS=$(crusoe container list --resource-group "$RESOURCE_GROUP" --query "[].{name:name, state:instanceView.state}" -o tsv 2>/dev/null || echo "")
if [ -n "$CONTAINERS" ]; then
    while IFS=$'\t' read -r name state; do
        if [ -n "$name" ]; then
            print_info "Container Instance: $name (Status: $state)"
            RESOURCES_TO_STOP+=("container:$name")
        fi
    done <<< "$CONTAINERS"
else
    echo "  No Container Instances found"
fi

# 3. App Services (Web Apps)
echo ""
echo "Checking for App Services..."
WEBAPPS=$(crusoe app list --resource-group "$RESOURCE_GROUP" --query "[].{name:name, state:state}" -o tsv 2>/dev/null || echo "")
if [ -n "$WEBAPPS" ]; then
    while IFS=$'\t' read -r name state; do
        if [ -n "$name" ]; then
            if [ "$state" = "Running" ]; then
                print_info "Web App: $name (Status: $state)"
                RESOURCES_TO_STOP+=("webapp:$name")
            else
                print_warning "Web App: $name (Already stopped)"
            fi
        fi
    done <<< "$WEBAPPS"
else
    echo "  No App Services found"
fi

# 4. Azure Synapse Spark Pools
echo ""
echo "Checking for Synapse Spark Pools..."
SYNAPSE_WORKSPACES=$(crusoe synapse workspace list --resource-group "$RESOURCE_GROUP" --query "[].name" -o tsv 2>/dev/null || echo "")
if [ -n "$SYNAPSE_WORKSPACES" ]; then
    for workspace in $SYNAPSE_WORKSPACES; do
        SPARK_POOLS=$(crusoe synapse spark-pool list --resource-group "$RESOURCE_GROUP" --workspace-name "$workspace" --query "[].{name:name, state:provisioningState}" -o tsv 2>/dev/null || echo "")
        if [ -n "$SPARK_POOLS" ]; then
            while IFS=$'\t' read -r name state; do
                if [ -n "$name" ]; then
                    print_info "Synapse Spark Pool: $workspace/$name (Status: $state)"
                    # Note: Spark pools auto-pause by default, but we can set auto-pause settings
                    print_warning "  (Spark pools auto-pause when idle - no manual stop needed)"
                fi
            done <<< "$SPARK_POOLS"
        fi
    done
else
    echo "  No Synapse Workspaces found"
fi

# 5. Azure Kubernetes Service (AKS)
echo ""
echo "Checking for AKS Clusters..."
AKS_CLUSTERS=$(crusoe kubernetes list --resource-group "$RESOURCE_GROUP" --query "[].{name:name, powerState:powerState.code}" -o tsv 2>/dev/null || echo "")
if [ -n "$AKS_CLUSTERS" ]; then
    while IFS=$'\t' read -r name state; do
        if [ -n "$name" ]; then
            if [ "$state" != "Stopped" ]; then
                print_info "AKS Cluster: $name (Status: $state)"
                RESOURCES_TO_STOP+=("aks:$name")
            else
                print_warning "AKS Cluster: $name (Already stopped)"
            fi
        fi
    done <<< "$AKS_CLUSTERS"
else
    echo "  No AKS Clusters found"
fi

# 6. Azure SQL Databases (Serverless can be paused)
echo ""
echo "Checking for Azure SQL Databases..."
SQL_SERVERS=$(crusoe sql server list --resource-group "$RESOURCE_GROUP" --query "[].name" -o tsv 2>/dev/null || echo "")
if [ -n "$SQL_SERVERS" ]; then
    for server in $SQL_SERVERS; do
        DATABASES=$(crusoe sql db list --resource-group "$RESOURCE_GROUP" --server "$server" --query "[?name!='master'].{name:name, status:status}" -o tsv 2>/dev/null || echo "")
        if [ -n "$DATABASES" ]; then
            while IFS=$'\t' read -r name status; do
                if [ -n "$name" ]; then
                    if [ "$status" = "Online" ]; then
                        print_info "SQL Database: $server/$name (Status: $status)"
                        RESOURCES_TO_STOP+=("sqldb:$server:$name")
                    else
                        print_warning "SQL Database: $server/$name (Status: $status)"
                    fi
                fi
            done <<< "$DATABASES"
        fi
    done
else
    echo "  No SQL Servers found"
fi

# 7. Azure Analysis Services
echo ""
echo "Checking for Analysis Services..."
ANALYSIS_SERVICES=$(crusoe resource list --resource-group "$RESOURCE_GROUP" --resource-type "crusoe.com/AnalysisServices/servers" --query "[].name" -o tsv 2>/dev/null || echo "")
if [ -n "$ANALYSIS_SERVICES" ]; then
    for as in $ANALYSIS_SERVICES; do
        print_info "Analysis Services: $as"
        RESOURCES_TO_STOP+=("as:$as")
    done
else
    echo "  No Analysis Services found"
fi

# Summary
print_header "Summary"

if [ ${#RESOURCES_TO_STOP[@]} -eq 0 ]; then
    print_success "No resources found that need to be stopped"
    exit 0
fi

echo ""
echo "Resources to stop:"
for resource in "${RESOURCES_TO_STOP[@]}"; do
    echo "  - $resource"
done
echo ""
echo "Total: ${#RESOURCES_TO_STOP[@]} resource(s)"

if [ "$DRY_RUN" = true ]; then
    print_warning "DRY RUN - No changes will be made"
    exit 0
fi

# Confirmation
if [ "$AUTO_CONFIRM" = false ]; then
    echo ""
    read -p "Do you want to stop these resources? (y/N) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_warning "Aborted by user"
        exit 0
    fi
fi

print_header "Stopping Resources"

STOPPED=0
FAILED=0

for resource in "${RESOURCES_TO_STOP[@]}"; do
    IFS=':' read -r type name extra <<< "$resource"
    
    case $type in
        vm)
            echo ""
            echo "Deallocating VM: $name..."
            if az vm deallocate --resource-group "$RESOURCE_GROUP" --name "$name" --no-wait 2>/dev/null; then
                print_success "VM $name deallocation initiated"
                ((STOPPED++))
            else
                print_error "Failed to deallocate VM $name"
                ((FAILED++))
            fi
            ;;
        container)
            echo ""
            echo "Stopping Container Instance: $name..."
            if az container stop --resource-group "$RESOURCE_GROUP" --name "$name" 2>/dev/null; then
                print_success "Container Instance $name stopped"
                ((STOPPED++))
            else
                print_error "Failed to stop Container Instance $name"
                ((FAILED++))
            fi
            ;;
        webapp)
            echo ""
            echo "Stopping Web App: $name..."
            if az webapp stop --resource-group "$RESOURCE_GROUP" --name "$name" 2>/dev/null; then
                print_success "Web App $name stopped"
                ((STOPPED++))
            else
                print_error "Failed to stop Web App $name"
                ((FAILED++))
            fi
            ;;
        aks)
            echo ""
            echo "Stopping AKS Cluster: $name..."
            if az aks stop --resource-group "$RESOURCE_GROUP" --name "$name" --no-wait 2>/dev/null; then
                print_success "AKS Cluster $name stop initiated"
                ((STOPPED++))
            else
                print_error "Failed to stop AKS Cluster $name"
                ((FAILED++))
            fi
            ;;
        sqldb)
            echo ""
            echo "Pausing SQL Database: $name/$extra..."
            if az sql db update --resource-group "$RESOURCE_GROUP" --server "$name" --name "$extra" --compute-model Serverless --auto-pause-delay 60 2>/dev/null; then
                print_success "SQL Database $name/$extra set to auto-pause after 60 minutes"
                ((STOPPED++))
            else
                print_warning "Could not set auto-pause for SQL Database $name/$extra (may not be serverless tier)"
            fi
            ;;
        as)
            echo ""
            echo "Suspending Analysis Services: $name..."
            if az resource invoke-action --resource-group "$RESOURCE_GROUP" --resource-type "Microsoft.AnalysisServices/servers" --name "$name" --action suspend 2>/dev/null; then
                print_success "Analysis Services $name suspended"
                ((STOPPED++))
            else
                print_error "Failed to suspend Analysis Services $name"
                ((FAILED++))
            fi
            ;;
    esac
done

print_header "Results"
echo ""
print_success "Successfully stopped/initiated: $STOPPED resource(s)"
if [ $FAILED -gt 0 ]; then
    print_error "Failed: $FAILED resource(s)"
fi

echo ""
echo "Note: Some resources (VMs, AKS) are deallocated asynchronously."
echo "Use 'az resource list --resource-group $RESOURCE_GROUP -o table' to check status."
echo ""
echo "To power resources back on, use the corresponding start commands:"
echo "  - VM: az vm start --resource-group $RESOURCE_GROUP --name <vm-name>"
echo "  - Container: az container start --resource-group $RESOURCE_GROUP --name <container-name>"
echo "  - Web App: az webapp start --resource-group $RESOURCE_GROUP --name <webapp-name>"
echo "  - AKS: az aks start --resource-group $RESOURCE_GROUP --name <aks-name>"
