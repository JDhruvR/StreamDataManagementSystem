/**
 * Chart Renderer - Render charts using Plotly
 */

class ChartRenderer {
    /**
     * Render a chart in a container
     * @param {String} containerId - DOM element ID
     * @param {Array} traces - Plotly trace objects
     * @param {Object} layout - Plotly layout configuration
     */
    static renderChart(containerId, traces, layout) {
        const container = document.getElementById(containerId);
        if (!container) {
            console.error(`Container '${containerId}' not found`);
            return;
        }

        try {
            const defaultLayout = {
                responsive: true,
                autosize: true,
                hovermode: 'closest',
                margin: { l: 50, r: 50, t: 50, b: 50 },
                ...layout
            };

            Plotly.newPlot(containerId, traces, defaultLayout, {
                responsive: true,
                displayModeBar: true,
                displaylogo: false,
                modeBarButtonsToRemove: ['lasso2d', 'select2d'],
            });
        } catch (error) {
            console.error('Error rendering chart:', error);
            container.innerHTML = `<div style="padding: 20px; color: red;">Error rendering chart: ${error.message}</div>`;
        }
    }

    /**
     * Clear a chart
     * @param {String} containerId - DOM element ID
     */
    static clearChart(containerId) {
        const container = document.getElementById(containerId);
        if (container) {
            Plotly.purge(containerId);
            container.innerHTML = '<p style="text-align: center; color: #999;">No data to display</p>';
        }
    }

    /**
     * Render from chart data object
     * @param {String} containerId - DOM element ID
     * @param {Object} chartData - Object with {data, layout}
     */
    static renderFromChartData(containerId, chartData) {
        if (chartData && chartData.data) {
            this.renderChart(containerId, chartData.data, chartData.layout);
        } else {
            this.clearChart(containerId);
        }
    }

    /**
     * Render a data table
     * @param {String} containerId - DOM element ID
     * @param {Array} fields - Column names
     * @param {Array} rows - Data rows
     * @param {Number} maxRows - Max rows to display
     */
    static renderTable(containerId, fields, rows, maxRows = 10) {
        const container = document.getElementById(containerId);
        if (!container) {
            console.error(`Container '${containerId}' not found`);
            return;
        }

        if (fields.length === 0 || rows.length === 0) {
            container.innerHTML = '<p style="color: #999;">No data available</p>';
            return;
        }

        let html = '<table class="data-table"><thead><tr>';
        for (const field of fields) {
            html += `<th>${this.escapeHtml(field)}</th>`;
        }
        html += '</tr></thead><tbody>';

        const displayRows = rows.slice(0, maxRows);
        for (const row of displayRows) {
            html += '<tr>';
            for (const cell of row) {
                html += `<td>${this.escapeHtml(cell)}</td>`;
            }
            html += '</tr>';
        }

        html += '</tbody></table>';
        container.innerHTML = html;
    }

    /**
     * Render info panel
     * @param {String} containerId - DOM element ID
     * @param {Object} info - Info object with key-value pairs
     */
    static renderInfo(containerId, info) {
        const container = document.getElementById(containerId);
        if (!container) {
            console.error(`Container '${containerId}' not found`);
            return;
        }

        if (!info || Object.keys(info).length === 0) {
            container.innerHTML = '<p style="color: #999;">No information available</p>';
            return;
        }

        let html = '';
        for (const [key, value] of Object.entries(info)) {
            const displayValue = typeof value === 'object' ? JSON.stringify(value) : value;
            html += `
                <div class="info-item">
                    <span class="info-label">${this.escapeHtml(key)}:</span>
                    <span class="info-value">${this.escapeHtml(String(displayValue))}</span>
                </div>
            `;
        }
        container.innerHTML = html;
    }

    /**
     * Escape HTML special characters
     * @param {String} text - Text to escape
     * @returns {String} Escaped text
     */
    static escapeHtml(text) {
        const map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return String(text).replace(/[&<>"']/g, m => map[m]);
    }

    /**
     * Render schema information as HTML
     * @param {String} containerId - DOM element ID
     * @param {Object} schema - Schema object
     */
    static renderSchema(containerId, schema) {
        const container = document.getElementById(containerId);
        if (!container) {
            console.error(`Container '${containerId}' not found`);
            return;
        }

        if (!schema) {
            container.innerHTML = '<p style="color: #999;">No schema available</p>';
            return;
        }

        let html = '';

        if (schema.schema_name) {
            html += `<div class="schema-item">
                <h4>Schema Name</h4>
                <pre>${this.escapeHtml(schema.schema_name)}</pre>
            </div>`;
        }

        if (schema.window_config) {
            html += `<div class="schema-item">
                <h4>Window Configuration</h4>
                <pre>${JSON.stringify(schema.window_config, null, 2)}</pre>
            </div>`;
        }

        if (schema.input_streams && Object.keys(schema.input_streams).length > 0) {
            html += `<div class="schema-item">
                <h4>Input Streams</h4>
                <pre>${JSON.stringify(schema.input_streams, null, 2)}</pre>
            </div>`;
        }

        if (schema.output_streams && Object.keys(schema.output_streams).length > 0) {
            html += `<div class="schema-item">
                <h4>Output Streams</h4>
                <pre>${JSON.stringify(schema.output_streams, null, 2)}</pre>
            </div>`;
        }

        container.innerHTML = html || '<p style="color: #999;">Empty schema</p>';
    }
}
