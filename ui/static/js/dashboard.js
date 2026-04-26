/**
 * Dashboard - Main application logic and event handling
 */

class Dashboard {
    constructor() {
        this.currentQuery = null;
        this.currentVizType = 'timeseries';
        this.currentDataSource = 'live';
        this.autoRefreshEnabled = true;
        this.refreshInterval = 1000;
        this.autoRefreshTimer = null;
        this.schemaData = null;

        this.init();
    }

    init() {
        console.log('Initializing Dashboard...');
        this.bindEvents();
        this.loadQueries();
        this.updateStatus();
        this.startAutoRefresh();
    }

    bindEvents() {
        // Control panel events
        document.getElementById('query-selector').addEventListener('change', (e) => {
            this.currentQuery = e.target.value;
            if (this.currentQuery) {
                this.refreshData();
            }
        });

        document.getElementById('viz-selector').addEventListener('change', (e) => {
            this.currentVizType = e.target.value;
            if (this.currentQuery) {
                this.refreshData();
            }
        });

        document.getElementById('data-source').addEventListener('change', (e) => {
            this.currentDataSource = e.target.value;
            if (this.currentQuery) {
                this.refreshData();
            }
        });

        document.getElementById('refresh-interval').addEventListener('change', (e) => {
            this.refreshInterval = Math.max(500, parseInt(e.target.value));
            this.restartAutoRefresh();
        });

        document.getElementById('auto-refresh-toggle').addEventListener('change', (e) => {
            this.autoRefreshEnabled = e.target.checked;
            if (this.autoRefreshEnabled) {
                this.startAutoRefresh();
            } else {
                this.stopAutoRefresh();
            }
        });

        document.getElementById('refresh-button').addEventListener('click', () => {
            if (this.currentQuery) {
                this.refreshData();
            }
        });

        document.getElementById('schema-button').addEventListener('click', () => {
            this.showSchemaModal();
        });

        // Modal close button
        document.querySelector('.close').addEventListener('click', () => {
            document.getElementById('schema-modal').style.display = 'none';
        });

        window.addEventListener('click', (event) => {
            const modal = document.getElementById('schema-modal');
            if (event.target === modal) {
                modal.style.display = 'none';
            }
        });

        console.log('Events bound');
    }

    /**
     * Load and populate query selector
     */
    async loadQueries() {
        try {
            const response = await fetch('/api/queries');
            if (!response.ok) {
                console.error('Failed to load queries:', response.statusText);
                this.showError('Failed to load queries');
                return;
            }

            const data = await response.json();
            const queries = data.queries || [];

            const selector = document.getElementById('query-selector');
            selector.innerHTML = '';

            if (queries.length === 0) {
                selector.innerHTML = '<option value="">No queries available</option>';
                this.updateStatus('No active queries');
                return;
            }

            for (const query of queries) {
                const option = document.createElement('option');
                option.value = query.name;
                option.textContent = `${query.name} (${query.output_stream})`;
                selector.appendChild(option);
            }

            // Auto-select first query
            if (queries.length > 0) {
                selector.value = queries[0].name;
                this.currentQuery = queries[0].name;
                this.refreshData();
            }

            this.updateStatus(`Loaded ${queries.length} queries`);
        } catch (error) {
            console.error('Error loading queries:', error);
            this.showError(`Error loading queries: ${error.message}`);
        }
    }

    /**
     * Fetch and display data for current query
     */
    async refreshData() {
        if (!this.currentQuery) {
            ChartRenderer.clearChart('chart');
            return;
        }

        try {
            const endpoint = this.currentDataSource === 'live'
                ? `/api/query/${this.currentQuery}/live?limit=100`
                : `/api/query/${this.currentQuery}/history?limit=100`;

            const response = await fetch(endpoint);
            if (!response.ok) {
                console.error('Failed to fetch data:', response.statusText);
                this.showError(`Failed to fetch data: ${response.statusText}`);
                return;
            }

            const data = await response.json();

            if (data.error) {
                this.showError(data.error);
                ChartRenderer.clearChart('chart');
                this.updateQueryInfo({});
                this.updateBufferStatus({});
                this.updateDataTable([]);
                return;
            }

            // Map data to chart
            const chartData = DataMapper.mapEventsToChartData(data.data, this.currentVizType);
            ChartRenderer.renderFromChartData('chart', chartData);

            // Update query info
            this.updateQueryInfo(data);

            // Update buffer status
            if (data.buffer_info) {
                this.updateBufferStatus(data.buffer_info);
            }

            // Update data table
            const { fields, rows } = DataMapper.formatEventsAsTable(data.data);
            this.updateDataTable(fields, rows);

            this.updateStatus(`Updated ${data.data.length} events`);
        } catch (error) {
            console.error('Error refreshing data:', error);
            this.showError(`Error refreshing data: ${error.message}`);
        }
    }

    /**
     * Update query info panel
     */
    updateQueryInfo(data) {
        if (!data || Object.keys(data).length === 0) {
            ChartRenderer.renderInfo('query-info', {
                'Status': 'No query selected',
            });
            return;
        }

        const info = {
            'Query Name': data.query_name || 'N/A',
            'Output Topic': data.output_topic || 'N/A',
            'Data Points': data.count || 0,
            'Data Source': this.currentDataSource === 'live' ? 'Kafka (Live)' : 'SQLite (History)',
            'Viz Type': this.currentVizType,
        };

        if (data.table_name) {
            info['Table Name'] = data.table_name;
            info['Total Rows'] = data.row_count || 0;
        }

        ChartRenderer.renderInfo('query-info', info);
    }

    /**
     * Update buffer status panel
     */
    updateBufferStatus(bufferInfo) {
        if (!bufferInfo) {
            ChartRenderer.renderInfo('buffer-status', {
                'Status': 'No buffer info',
            });
            return;
        }

        const info = {
            'Events in Buffer': bufferInfo.count || 0,
            'Buffer Capacity': bufferInfo.capacity || 0,
            'Fill Rate': `${Math.round((bufferInfo.count / bufferInfo.capacity) * 100)}%`,
            'Last Update': bufferInfo.last_update ? new Date(bufferInfo.last_update).toLocaleTimeString() : 'N/A',
        };

        ChartRenderer.renderInfo('buffer-status', info);
    }

    /**
     * Update data table
     */
    updateDataTable(fields, rows) {
        if (!fields || fields.length === 0) {
            document.getElementById('data-table').innerHTML = '<p style="color: #999;">No data</p>';
            return;
        }

        ChartRenderer.renderTable('data-table', fields, rows, 5);
    }

    /**
     * Show schema information modal
     */
    async showSchemaModal() {
        try {
            if (!this.schemaData) {
                const response = await fetch('/api/schema');
                if (!response.ok) {
                    this.showError('Failed to load schema');
                    return;
                }
                this.schemaData = await response.json();
            }

            ChartRenderer.renderSchema('schema-content', this.schemaData);
            document.getElementById('schema-modal').style.display = 'block';
        } catch (error) {
            console.error('Error showing schema:', error);
            this.showError(`Error loading schema: ${error.message}`);
        }
    }

    /**
     * Update system status indicator
     */
    async updateStatus(message) {
        try {
            const response = await fetch('/api/status');
            const status = await response.json();

            const indicator = document.getElementById('status-indicator');
            const statusText = document.getElementById('status-text');
            const footerStatus = document.getElementById('footer-status');

            if (status.ok) {
                indicator.className = 'status-dot';
                statusText.textContent = message || 'System Ready';
                footerStatus.textContent = 'Ready';
            } else {
                indicator.className = 'status-dot error';
                statusText.textContent = 'System Error';
                footerStatus.textContent = 'Error';
            }
        } catch (error) {
            const indicator = document.getElementById('status-indicator');
            const statusText = document.getElementById('status-text');

            indicator.className = 'status-dot error';
            statusText.textContent = 'Connection Error';
            console.error('Error updating status:', error);
        }
    }

    /**
     * Show error message
     */
    showError(message) {
        console.error('Dashboard Error:', message);
        // You could add a toast notification here
    }

    /**
     * Start auto-refresh timer
     */
    startAutoRefresh() {
        this.stopAutoRefresh();

        if (!this.autoRefreshEnabled) {
            return;
        }

        this.autoRefreshTimer = setInterval(() => {
            if (this.currentQuery) {
                this.refreshData();
            }
        }, this.refreshInterval);

        console.log(`Auto-refresh started (${this.refreshInterval}ms)`);
    }

    /**
     * Stop auto-refresh timer
     */
    stopAutoRefresh() {
        if (this.autoRefreshTimer) {
            clearInterval(this.autoRefreshTimer);
            this.autoRefreshTimer = null;
            console.log('Auto-refresh stopped');
        }
    }

    /**
     * Restart auto-refresh with new interval
     */
    restartAutoRefresh() {
        if (this.autoRefreshEnabled) {
            this.startAutoRefresh();
        }
    }
}

// Initialize dashboard when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM Content Loaded, initializing Dashboard');
    window.dashboard = new Dashboard();
});
