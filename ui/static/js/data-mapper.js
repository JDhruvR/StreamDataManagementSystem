/**
 * Data Mapper - Transform query output events into chart-ready data
 */

class DataMapper {
    /**
     * Parse and map event data for visualization
     * @param {Array} events - Array of event objects
     * @param {String} vizType - Visualization type (timeseries, bar, gauge, heatmap)
     * @returns {Object} Chart data in Plotly format
     */
    static mapEventsToChartData(events, vizType) {
        if (!events || events.length === 0) {
            return this.getEmptyChartData(vizType);
        }

        switch (vizType) {
            case 'timeseries':
                return this.mapToTimeSeries(events);
            case 'bar':
                return this.mapToBarChart(events);
            case 'gauge':
                return this.mapToGauge(events);
            case 'heatmap':
                return this.mapToHeatmap(events);
            default:
                return this.mapToTimeSeries(events);
        }
    }

    /**
     * Map events to time-series data
     * @param {Array} events - Event objects
     * @returns {Object} Plotly trace and layout
     */
    static mapToTimeSeries(events) {
        if (events.length === 0) {
            return { data: [], layout: { title: 'No data available' } };
        }

        // Extract numeric fields and timestamps
        const numericFields = this.extractNumericFields(events);
        const timestamps = this.extractTimestamps(events);

        const traces = [];
        for (const field of numericFields.slice(0, 5)) {  // Limit to 5 series
            const values = events.map(e => this.getNestedValue(e, field));
            traces.push({
                x: timestamps,
                y: values,
                type: 'scatter',
                mode: 'lines+markers',
                name: field,
                line: { width: 2 },
            });
        }

        const layout = {
            title: 'Time-Series Data',
            xaxis: { title: 'Time', type: 'date' },
            yaxis: { title: 'Value' },
            hovermode: 'x unified',
            height: 500,
        };

        return { data: traces, layout };
    }

    /**
     * Map events to bar chart data (grouped aggregations)
     * @param {Array} events - Event objects
     * @returns {Object} Plotly trace and layout
     */
    static mapToBarChart(events) {
        if (events.length === 0) {
            return { data: [], layout: { title: 'No data available' } };
        }

        // Find category fields (strings) and numeric aggregates
        const categoryField = this.findCategoryField(events);
        const numericFields = this.extractNumericFields(events);

        if (!categoryField || numericFields.length === 0) {
            // Fallback: count events by first string field
            const grouping = this.groupEventsByField(events, categoryField || Object.keys(events[0])[0]);
            const categories = Object.keys(grouping);
            const counts = Object.keys(grouping).map(k => grouping[k].length);

            return {
                data: [{
                    x: categories,
                    y: counts,
                    type: 'bar',
                    marker: { color: '#3498db' },
                }],
                layout: {
                    title: 'Event Count by Category',
                    xaxis: { title: categoryField },
                    yaxis: { title: 'Count' },
                    height: 500,
                }
            };
        }

        // Group by category and aggregate numeric fields
        const grouping = this.groupEventsByField(events, categoryField);
        const categories = Object.keys(grouping);

        const traces = [];
        for (const field of numericFields.slice(0, 3)) {
            const values = categories.map(cat => {
                const groupEvents = grouping[cat];
                const vals = groupEvents.map(e => this.getNestedValue(e, field)).filter(v => v !== null && !isNaN(v));
                return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : 0;
            });

            traces.push({
                x: categories,
                y: values,
                type: 'bar',
                name: field,
            });
        }

        return {
            data: traces,
            layout: {
                title: 'Aggregated Values by Category',
                xaxis: { title: categoryField },
                yaxis: { title: 'Average Value' },
                barmode: 'group',
                height: 500,
            }
        };
    }

    /**
     * Map to real-time gauge (latest value)
     * @param {Array} events - Event objects
     * @returns {Object} Plotly indicator trace
     */
    static mapToGauge(events) {
        if (events.length === 0) {
            return { data: [], layout: { title: 'No data available' } };
        }

        // Get latest event and first numeric field
        const latestEvent = events[events.length - 1];
        const numericFields = this.extractNumericFields([latestEvent]);

        if (numericFields.length === 0) {
            return { data: [], layout: { title: 'No numeric data available' } };
        }

        const field = numericFields[0];
        const value = this.getNestedValue(latestEvent, field);

        // Determine gauge range
        const allValues = events
            .map(e => this.getNestedValue(e, field))
            .filter(v => v !== null && !isNaN(v));

        const minVal = Math.min(...allValues);
        const maxVal = Math.max(...allValues);
        const range = maxVal - minVal;

        return {
            data: [{
                type: 'indicator',
                mode: 'gauge+number+delta',
                value: value,
                title: { text: field },
                delta: { reference: allValues[Math.max(0, allValues.length - 2)] },
                gauge: {
                    axis: { range: [minVal - range * 0.1, maxVal + range * 0.1] },
                    bar: { color: 'darkblue' },
                    steps: [
                        { range: [minVal, minVal + range * 0.33], color: 'lightgray' },
                        { range: [minVal + range * 0.33, minVal + range * 0.66], color: 'gray' }
                    ],
                    threshold: {
                        line: { color: 'red', width: 4 },
                        thickness: 0.75,
                        value: maxVal
                    }
                }
            }],
            layout: {
                height: 500,
                margin: { l: 50, r: 25, t: 25, b: 25 }
            }
        };
    }

    /**
     * Map to heatmap (correlations or temporal patterns)
     * @param {Array} events - Event objects
     * @returns {Object} Plotly heatmap trace
     */
    static mapToHeatmap(events) {
        if (events.length === 0) {
            return { data: [], layout: { title: 'No data available' } };
        }

        // Create a simple time-bucket x value heatmap
        const numericFields = this.extractNumericFields(events);
        if (numericFields.length < 2) {
            return { data: [], layout: { title: 'Need at least 2 numeric fields' } };
        }

        // Create matrix: fields x time buckets
        const field1 = numericFields[0];
        const field2 = numericFields[1];

        // Normalize field1 values for x-axis (5 buckets)
        const field1Vals = events.map(e => this.getNestedValue(e, field1)).filter(v => v !== null && !isNaN(v));
        const field1Min = Math.min(...field1Vals);
        const field1Max = Math.max(...field1Vals);
        const field1Range = field1Max - field1Min || 1;

        // Create heatmap data
        const buckets = 5;
        const heatmapData = Array(buckets).fill(0).map(() => Array(buckets).fill(0));
        const bucketCounts = Array(buckets).fill(0).map(() => Array(buckets).fill(0));

        for (const event of events) {
            const f1Val = this.getNestedValue(event, field1);
            const f2Val = this.getNestedValue(event, field2);

            if (f1Val === null || f2Val === null || isNaN(f1Val) || isNaN(f2Val)) continue;

            const f1Bucket = Math.min(buckets - 1, Math.floor(((f1Val - field1Min) / field1Range) * buckets));
            const f2Bucket = Math.min(buckets - 1, Math.floor(((f2Val - field1Min) / field1Range) * buckets));

            heatmapData[f2Bucket][f1Bucket] += f2Val;
            bucketCounts[f2Bucket][f1Bucket]++;
        }

        // Average the values
        for (let i = 0; i < buckets; i++) {
            for (let j = 0; j < buckets; j++) {
                if (bucketCounts[i][j] > 0) {
                    heatmapData[i][j] /= bucketCounts[i][j];
                }
            }
        }

        return {
            data: [{
                z: heatmapData,
                type: 'heatmap',
                colorscale: 'Viridis',
                name: `${field1} vs ${field2}`,
            }],
            layout: {
                title: `Heatmap: ${field1} vs ${field2}`,
                xaxis: { title: field1 },
                yaxis: { title: field2 },
                height: 500,
            }
        };
    }

    /**
     * Extract numeric fields from events
     * @param {Array} events - Event objects
     * @returns {Array} Field names that contain numeric values
     */
    static extractNumericFields(events) {
        if (events.length === 0) return [];

        const fields = new Set();
        for (const event of events) {
            for (const [key, value] of Object.entries(event)) {
                if (key.startsWith('_sdms_')) continue;  // Skip metadata
                if (typeof value === 'number' || (typeof value === 'string' && !isNaN(value))) {
                    fields.add(key);
                }
            }
        }
        return Array.from(fields);
    }

    /**
     * Find a category field (string, non-numeric)
     * @param {Array} events - Event objects
     * @returns {String|null} Field name or null
     */
    static findCategoryField(events) {
        if (events.length === 0) return null;

        const event = events[0];
        for (const [key, value] of Object.entries(event)) {
            if (key.startsWith('_sdms_')) continue;
            if (typeof value === 'string' || (typeof value === 'number' && isNaN(value))) {
                return key;
            }
        }
        return null;
    }

    /**
     * Extract timestamps from events
     * @param {Array} events - Event objects
     * @returns {Array} ISO timestamp strings
     */
    static extractTimestamps(events) {
        return events.map(e => {
            const ts = e._sdms_received_at || e.timestamp || new Date().toISOString();
            return ts;
        });
    }

    /**
     * Group events by a field value
     * @param {Array} events - Event objects
     * @param {String} fieldName - Field to group by
     * @returns {Object} Grouped events
     */
    static groupEventsByField(events, fieldName) {
        const grouping = {};
        for (const event of events) {
            const key = String(this.getNestedValue(event, fieldName));
            if (!grouping[key]) {
                grouping[key] = [];
            }
            grouping[key].push(event);
        }
        return grouping;
    }

    /**
     * Get nested value from object using dot notation
     * @param {Object} obj - Object to query
     * @param {String} path - Path like 'field' or 'nested.field'
     * @returns {*} Value or null
     */
    static getNestedValue(obj, path) {
        try {
            const parts = path.split('.');
            let value = obj;
            for (const part of parts) {
                value = value[part];
                if (value === undefined) return null;
            }
            return value;
        } catch {
            return null;
        }
    }

    /**
     * Get empty chart data for a visualization type
     * @param {String} vizType - Visualization type
     * @returns {Object} Empty chart data
     */
    static getEmptyChartData(vizType) {
        return {
            data: [],
            layout: {
                title: 'No data available',
                xaxis: { title: 'X' },
                yaxis: { title: 'Y' },
                height: 500,
            }
        };
    }

    /**
     * Format event data into table rows
     * @param {Array} events - Event objects
     * @param {Number} limit - Max rows to display
     * @returns {Array} Array of field names and rows
     */
    static formatEventsAsTable(events, limit = 10) {
        if (events.length === 0) {
            return { fields: [], rows: [] };
        }

        // Get field names from first event (excluding metadata)
        const fields = Object.keys(events[0]).filter(k => !k.startsWith('_sdms_'));

        // Format rows
        const rows = events.slice(-limit).reverse().map(event => {
            return fields.map(field => {
                const value = event[field];
                if (value === null || value === undefined) return '-';
                if (typeof value === 'number') return value.toFixed(2);
                return String(value).substring(0, 30);
            });
        });

        return { fields, rows };
    }
}
