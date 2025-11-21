/**
 * WeatherProject Dashboard - Charts & Visualizations
 * Gestion des graphiques et interactions
 */

// Configuration globale
const API_BASE_URL = window.location.origin;
let currentData = {};

// Instances des graphiques (pour pouvoir les détruire/recréer)
let tempDistributionChartInstance = null;

/**
 * Initialise le dashboard
 */
async function initializeDashboard() {
    console.log('Initialisation du dashboard...');
    
    // Charger les données
    await loadAllData();
    
    // Initialiser les graphiques
    initializeCharts();
    
    // Initialiser les event listeners
    initializeEventListeners();
    
    // Navigation smooth
    initializeSmoothScroll();
    
    console.log('Dashboard initialisé avec succès');
}

/**
 * Charge toutes les données nécessaires
 */
async function loadAllData() {
    try {
        showLoading(true);
        
        // Charger les statistiques
        await loadStatistics();
        
        // Charger les données météo
        await loadWeatherData();
        
        // Charger les métadonnées
        await loadIngestionMetadata();
        
        showLoading(false);
    } catch (error) {
        console.error('Erreur lors du chargement des données:', error);
        showError('Impossible de charger les données');
        showLoading(false);
    }
}

/**
 * Charge les statistiques générales
 */
async function loadStatistics() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/weather/statistics`);
        const data = await response.json();
        
        if (data.success && data.statistics) {
            updateStatisticsCards(data.statistics);
        }
    } catch (error) {
        console.error('Erreur chargement statistiques:', error);
    }
}

/**
 * Met à jour les cartes de statistiques
 */
function updateStatisticsCards(stats) {
    // Température
    if (stats.temperature) {
        document.getElementById('avgTemp').textContent = 
            stats.temperature.mean.toFixed(1);
    }
    
    // Précipitations
    if (stats.precipitation) {
        document.getElementById('totalPrecip').textContent = 
            stats.precipitation.total.toFixed(0);
    }
    
    // Humidité
    if (stats.humidity) {
        document.getElementById('avgHumidity').textContent = 
            stats.humidity.mean.toFixed(1);
    }
    
    // Période
    if (stats.period) {
        document.getElementById('totalDays').textContent = 
            stats.period.total_days;
    }
}

/**
 * Charge les données météo
 */
async function loadWeatherData() {
    try {
        // Données quotidiennes
        const dailyResponse = await fetch(`${API_BASE_URL}/api/weather/daily?limit=365`);
        const dailyData = await dailyResponse.json();
        
        if (dailyData.success) {
            currentData.daily = dailyData.data;
            updateDataTable(dailyData.data.slice(0, 50));
        }
        
        // Données mensuelles
        const monthlyResponse = await fetch(`${API_BASE_URL}/api/weather/monthly`);
        const monthlyData = await monthlyResponse.json();
        
        if (monthlyData.success) {
            currentData.monthly = monthlyData.data;
        }
        
        // Données saisonnières
        const seasonalResponse = await fetch(`${API_BASE_URL}/api/weather/seasonal`);
        const seasonalData = await seasonalResponse.json();
        
        if (seasonalData.success) {
            currentData.seasonal = seasonalData.data;
        }
        
    } catch (error) {
        console.error('Erreur chargement données météo:', error);
    }
}

/**
 * Initialise tous les graphiques
 */
function initializeCharts() {
    if (currentData.daily && currentData.daily.length > 0) {
        createTemperatureTrendChart();
        createTemperatureDistributionChart();
    }
    
    if (currentData.monthly && currentData.monthly.length > 0) {
        createMonthlyAveragesChart();
        createPrecipitationChart();
        createTemperatureHeatmap();
    }
    
    if (currentData.seasonal && currentData.seasonal.length > 0) {
        createSeasonalComparisonChart();
    }
}

/**
 * Graphique: Évolution de la température
 */
function createTemperatureTrendChart() {
    if (!currentData.daily || currentData.daily.length === 0) {
        console.warn('Pas de données pour créer le graphique de température');
        return;
    }
    const data = currentData.daily.slice().reverse().slice(-365);
    
    const trace1 = {
        x: data.map(d => d.DATE),
        y: data.map(d => d.TEMP_AVG),
        name: 'Temp. Moyenne',
        type: 'scatter',
        mode: 'lines',
        line: { color: '#2563eb', width: 2 }
    };
    
    const trace2 = {
        x: data.map(d => d.DATE),
        y: data.map(d => d.TEMP_MAX),
        name: 'Temp. Maximale',
        type: 'scatter',
        mode: 'lines',
        line: { color: '#ef4444', width: 1, dash: 'dot' }
    };
    
    const trace3 = {
        x: data.map(d => d.DATE),
        y: data.map(d => d.TEMP_MIN),
        name: 'Temp. Minimale',
        type: 'scatter',
        mode: 'lines',
        line: { color: '#06b6d4', width: 1, dash: 'dot' }
    };
    
    const layout = {
        title: '',
        xaxis: { title: 'Date' },
        yaxis: { title: 'Température (°C)' },
        hovermode: 'x unified',
        showlegend: true,
        legend: { orientation: 'h', y: -0.2 },
        margin: { l: 50, r: 50, t: 20, b: 50 }
    };
    
    Plotly.newPlot('temperatureTrendChart', [trace1, trace2, trace3], layout, {
        responsive: true,
        displayModeBar: true,
        displaylogo: false
    });
}

/**
 * Graphique: Moyennes mensuelles
 */
function createMonthlyAveragesChart() {
    const data = currentData.monthly.slice().reverse();
    
    const monthNames = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun', 
                       'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc'];
    
    const trace = {
        x: data.map(d => `${monthNames[d.MONTH - 1]} ${d.YEAR}`),
        y: data.map(d => d.TEMP_AVG),
        type: 'bar',
        marker: {
            color: data.map(d => d.TEMP_AVG),
            colorscale: 'RdYlBu',
            reversescale: true,
            showscale: true,
            colorbar: { title: 'Temp (°C)' }
        }
    };
    
    const layout = {
        title: '',
        xaxis: { title: 'Mois', tickangle: -45 },
        yaxis: { title: 'Température Moyenne (°C)' },
        margin: { l: 50, r: 50, t: 20, b: 100 }
    };
    
    Plotly.newPlot('monthlyAveragesChart', [trace], layout, {
        responsive: true,
        displayModeBar: false
    });
}

/**
 * Graphique: Comparaison saisonnière
 */
function createSeasonalComparisonChart() {
    const data = currentData.seasonal;
    
    // Grouper par saison
    const seasons = ['Winter', 'Spring', 'Summer', 'Fall'];
    const seasonColors = {
        'Winter': '#60a5fa',
        'Spring': '#34d399',
        'Summer': '#fbbf24',
        'Fall': '#f97316'
    };
    
    const traces = seasons.map(season => {
        const seasonData = data.filter(d => d.SEASON === season);
        return {
            x: seasonData.map(d => d.YEAR),
            y: seasonData.map(d => d.TEMP_AVG),
            name: season,
            type: 'scatter',
            mode: 'lines+markers',
            line: { color: seasonColors[season], width: 2 }
        };
    });
    
    const layout = {
        title: '',
        xaxis: { title: 'Année' },
        yaxis: { title: 'Température Moyenne (°C)' },
        showlegend: true,
        legend: { orientation: 'h', y: -0.2 },
        margin: { l: 50, r: 50, t: 20, b: 50 }
    };
    
    Plotly.newPlot('seasonalComparisonChart', traces, layout, {
        responsive: true,
        displayModeBar: false
    });
}

/**
 * Graphique: Heatmap des températures
 */
function createTemperatureHeatmap() {
    const data = currentData.monthly;
    
    // Préparer les données pour la heatmap
    const years = [...new Set(data.map(d => d.YEAR))].sort();
    const months = Array.from({ length: 12 }, (_, i) => i + 1);
    
    const zData = months.map(month => {
        return years.map(year => {
            const record = data.find(d => d.YEAR === year && d.MONTH === month);
            return record ? record.TEMP_AVG : null;
        });
    });
    
    const monthNames = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun', 
                       'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc'];
    
    const trace = {
        z: zData,
        x: years,
        y: monthNames,
        type: 'heatmap',
        colorscale: 'RdYlBu',
        reversescale: true,
        colorbar: { title: 'Temp (°C)' }
    };
    
    const layout = {
        title: '',
        xaxis: { title: 'Année' },
        yaxis: { title: 'Mois' },
        margin: { l: 80, r: 50, t: 20, b: 50 }
    };
    
    Plotly.newPlot('temperatureHeatmap', [trace], layout, {
        responsive: true,
        displayModeBar: false
    });
}

/**
 * Graphique: Précipitations mensuelles
 */
function createPrecipitationChart() {
    const data = currentData.monthly.slice().reverse().slice(-24);
    
    const monthNames = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun', 
                       'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc'];
    
    const trace = {
        x: data.map(d => `${monthNames[d.MONTH - 1]} ${d.YEAR}`),
        y: data.map(d => d.PRECIPITATION_TOTAL || 0),
        type: 'bar',
        marker: { color: '#06b6d4' }
    };
    
    const layout = {
        title: '',
        xaxis: { title: 'Mois', tickangle: -45 },
        yaxis: { title: 'Précipitations (mm)' },
        margin: { l: 50, r: 50, t: 20, b: 100 }
    };
    
    Plotly.newPlot('precipitationChart', [trace], layout, {
        responsive: true,
        displayModeBar: false
    });
}

/**
 * Graphique: Distribution des températures (Chart.js)
 */
function createTemperatureDistributionChart() {
    if (!currentData.daily || currentData.daily.length === 0) {
        console.warn('Pas de données pour créer la distribution de température');
        return;
    }
    const data = currentData.daily;
    const temps = data.map(d => d.TEMP_AVG).filter(t => t != null);
    
    // Créer des bins pour l'histogramme
    const bins = 20;
    const min = Math.min(...temps);
    const max = Math.max(...temps);
    const binSize = (max - min) / bins;
    
    const histogram = Array(bins).fill(0);
    const labels = [];
    
    for (let i = 0; i < bins; i++) {
        const binStart = min + i * binSize;
        const binEnd = binStart + binSize;
        labels.push(`${binStart.toFixed(1)}`);
        
        histogram[i] = temps.filter(t => t >= binStart && t < binEnd).length;
    }
    
    // Détruire l'ancien graphique s'il existe
    if (tempDistributionChartInstance) {
        tempDistributionChartInstance.destroy();
    }
    
    const ctx = document.getElementById('tempDistributionChart').getContext('2d');
    tempDistributionChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Fréquence',
                data: histogram,
                backgroundColor: 'rgba(37, 99, 235, 0.7)',
                borderColor: 'rgba(37, 99, 235, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: { display: false },
                title: { display: false }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Nombre de jours' }
                },
                x: {
                    title: { display: true, text: 'Température (°C)' }
                }
            }
        }
    });
}

/**
 * Met à jour la table de données
 */
function updateDataTable(data) {
    const tbody = document.getElementById('dataTableBody');
    
    if (!data || data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="loading-cell">Aucune donnée disponible</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.map(row => `
        <tr>
            <td>${row.DATE || '-'}</td>
            <td>${row.TEMP_AVG != null ? row.TEMP_AVG.toFixed(1) : '-'}</td>
            <td>${row.TEMP_MIN != null ? row.TEMP_MIN.toFixed(1) : '-'}</td>
            <td>${row.TEMP_MAX != null ? row.TEMP_MAX.toFixed(1) : '-'}</td>
            <td>${row.PRECIPITATION_TOTAL != null ? row.PRECIPITATION_TOTAL.toFixed(1) : '-'}</td>
            <td>${row.HUMIDITY_AVG != null ? row.HUMIDITY_AVG.toFixed(1) : '-'}</td>
        </tr>
    `).join('');
}

/**
 * Charge les métadonnées d'ingestion
 */
async function loadIngestionMetadata() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/ingestion-stats`);
        const data = await response.json();
        
        if (data.success && data.stats) {
            displayIngestionMetadata(data.stats);
        }
    } catch (error) {
        console.error('Erreur chargement métadonnées:', error);
    }
}

/**
 * Affiche les métadonnées d'ingestion
 */
function displayIngestionMetadata(stats) {
    const container = document.getElementById('ingestionMetadata');
    
    if (!stats || stats.length === 0) {
        container.innerHTML = '<p>Aucune métadonnée disponible</p>';
        return;
    }
    
    container.innerHTML = stats.map(stat => `
        <div class="metadata-item">
            <h4>${stat.source_type}</h4>
            <ul>
                <li><strong>Total ingestions:</strong> ${stat.total_ingestions}</li>
                <li><strong>Total enregistrements:</strong> ${stat.total_records || 0}</li>
                <li><strong>Score qualité moyen:</strong> ${stat.avg_quality_score ? stat.avg_quality_score.toFixed(1) : '-'}%</li>
                <li><strong>Dernière ingestion:</strong> ${stat.last_ingestion ? new Date(stat.last_ingestion).toLocaleString('fr-FR') : '-'}</li>
            </ul>
        </div>
    `).join('');
}

/**
 * Initialise les event listeners
 */
function initializeEventListeners() {
    // Filtre de période pour le graphique de température
    document.getElementById('tempPeriodSelect')?.addEventListener('change', (e) => {
        const days = parseInt(e.target.value);
        filterTemperatureChart(days);
    });
    
    // Bouton appliquer filtres
    document.getElementById('applyFiltersBtn')?.addEventListener('click', applyFilters);
    
    // Bouton rafraîchir
    document.getElementById('refreshDataBtn')?.addEventListener('click', () => {
        loadAllData();
    });
    
    // Bouton export CSV
    document.getElementById('exportCsvBtn')?.addEventListener('click', exportToCSV);
}

/**
 * Applique les filtres sur les données
 */
async function applyFilters() {
    const startDate = document.getElementById('startDateFilter').value;
    const endDate = document.getElementById('endDateFilter').value;
    const limit = document.getElementById('limitFilter').value || 365;
    
    console.log('🔍 Application des filtres:', { startDate, endDate, limit });
    
    try {
        showLoading(true);
        
        let url = `${API_BASE_URL}/api/weather/daily?limit=${limit}`;
        if (startDate) url += `&start_date=${startDate}`;
        if (endDate) url += `&end_date=${endDate}`;
        
        console.log('📡 URL de requête:', url);
        
        const response = await fetch(url);
        console.log('📥 Réponse reçue:', response.status);
        
        const data = await response.json();
        console.log('📊 Données reçues:', data);
        
        if (data.success) {
            // Mettre à jour les données actuelles
            currentData.daily = data.data;
            console.log(`✅ ${data.data.length} enregistrements chargés`);
            
            // Recharger la table
            updateDataTable(data.data.slice(0, 50));
            console.log('✅ Table mise à jour');
            
            // IMPORTANT : Recharger tous les graphiques avec les nouvelles données !
            if (data.data && data.data.length > 0) {
                console.log('🎨 Rechargement du graphique de température...');
                createTemperatureTrendChart();
                console.log('🎨 Rechargement de la distribution...');
                createTemperatureDistributionChart();
                console.log('✅ Graphiques rechargés');
            } else {
                console.warn('⚠️ Aucune donnée à afficher');
            }
            
            // Afficher un message de succès
            console.log(`✅ Filtres appliqués : ${data.data.length} jours affichés`);
        } else {
            console.error('❌ Réponse API avec success=false:', data);
            showError('Aucune donnée trouvée pour ces filtres');
        }
        
        showLoading(false);
    } catch (error) {
        console.error('❌ Erreur application filtres:', error);
        console.error('❌ Stack:', error.stack);
        showError('Erreur lors de l\'application des filtres: ' + error.message);
        showLoading(false);
    }
}

/**
 * Exporte les données en CSV
 */
function exportToCSV() {
    if (!currentData.daily || currentData.daily.length === 0) {
        alert('Aucune donnée à exporter');
        return;
    }
    
    const headers = ['Date', 'Temp Moy', 'Temp Min', 'Temp Max', 'Précipitations', 'Humidité'];
    const rows = currentData.daily.map(row => [
        row.DATE,
        row.TEMP_AVG,
        row.TEMP_MIN,
        row.TEMP_MAX,
        row.PRECIPITATION_TOTAL,
        row.HUMIDITY_AVG
    ]);
    
    const csvContent = [
        headers.join(','),
        ...rows.map(row => row.join(','))
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    
    link.setAttribute('href', url);
    link.setAttribute('download', `weather_data_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

/**
 * Navigation smooth scroll
 */
function initializeSmoothScroll() {
    document.querySelectorAll('.nav-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const targetId = link.getAttribute('href');
            const targetSection = document.querySelector(targetId);
            
            if (targetSection) {
                targetSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
                
                // Mettre à jour le lien actif
                document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
                link.classList.add('active');
            }
        });
    });
}

/**
 * Affiche/masque le chargement
 */
function showLoading(show) {
    // À implémenter si besoin d'un overlay de chargement global
    console.log(show ? 'Chargement...' : 'Chargement terminé');
}

/**
 * Affiche une erreur
 */
function showError(message) {
    console.error(message);
    alert(message);
}

/**
 * Filtre le graphique de température par période
 */
function filterTemperatureChart(days) {
    if (!currentData.daily) {
        console.warn('Aucune donnée disponible pour filtrer');
        return;
    }
    
    console.log(`Filtrage du graphique : ${days} derniers jours`);
    
    // Filtrer les données (du plus récent au plus ancien)
    const sortedData = currentData.daily.slice().sort((a, b) => 
        new Date(b.DATE) - new Date(a.DATE)
    );
    const filteredData = sortedData.slice(0, days).reverse();
    
    console.log(`Affichage de ${filteredData.length} jours`);
    
    // Mettre à jour le graphique
    const update = {
        x: [
            filteredData.map(d => d.DATE),
            filteredData.map(d => d.DATE),
            filteredData.map(d => d.DATE)
        ],
        y: [
            filteredData.map(d => d.TEMP_AVG),
            filteredData.map(d => d.TEMP_MAX),
            filteredData.map(d => d.TEMP_MIN)
        ]
    };
    
    Plotly.restyle('temperatureTrendChart', update);
}

