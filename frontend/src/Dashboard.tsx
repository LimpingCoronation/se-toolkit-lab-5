import React, { useEffect, useState, useMemo } from 'react';
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    BarElement,
    Title,
    Tooltip,
    Legend,
    ChartOptions,
    ChartData,
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';

// Register ChartJS components
ChartJS.register(
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    BarElement,
    Title,
    Tooltip,
    Legend
);

// --- Types ---

interface Lab {
    id: string;
    name: string;
}

interface ScoreBucket {
    range: string;
    count: number;
}

interface ScoresResponse {
    buckets: ScoreBucket[];
}

interface TimelinePoint {
    date: string;
    count: number;
}

interface TimelineResponse {
    data: TimelinePoint[];
}

interface TaskPassRate {
    taskName: string;
    passRate: number;
    totalSubmissions: number;
}

interface PassRatesResponse {
    tasks: TaskPassRate[];
}

// --- API Helpers ---

const getAuthToken = (): string | null => {
    if (typeof window === 'undefined') return null;
    return localStorage.getItem('api_key');
};

const fetchWithAuth = async <T,>(endpoint: string, params: Record<string, string>): Promise<T> => {
    const token = getAuthToken();
    if (!token) {
        throw new Error('Authentication token missing');
    }

    const url = new URL(endpoint, window.location.origin);
    Object.entries(params).forEach(([key, value]) => {
        url.searchParams.append(key, value);
    });

    const response = await fetch(url.toString(), {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
        },
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`API Error ${response.status}: ${errorText}`);
    }

    return response.json() as Promise<T>;
};

// --- Component ---

const Dashboard: React.FC = () => {
    const [labs, setLabs] = useState<Lab[]>([]);
    const [selectedLabId, setSelectedLabId] = useState<string>('');

    const [scores, setScores] = useState<ScoresResponse | null>(null);
    const [timeline, setTimeline] = useState<TimelineResponse | null>(null);
    const [passRates, setPassRates] = useState<PassRatesResponse | null>(null);

    const [loading, setLoading] = useState<boolean>(false);
    const [error, setError] = useState<string | null>(null);

    // Fetch available labs on mount
    useEffect(() => {
        const loadLabs = async () => {
            try {
                // Assuming a /labs endpoint exists to populate the dropdown
                // If this endpoint differs, adjust the type and URL accordingly
                const data = await fetchWithAuth<Lab[]>('/labs', {});
                setLabs(data);
                if (data.length > 0) {
                    setSelectedLabId(data[0].id);
                }
            } catch (err) {
                setError(err instanceof Error ? err.message : 'Failed to load labs');
            }
        };

        loadLabs();
    }, []);

    // Fetch analytics when selectedLabId changes
    useEffect(() => {
        const loadAnalytics = async () => {
            if (!selectedLabId) return;

            setLoading(true);
            setError(null);

            try {
                const [scoresData, timelineData, passRatesData] = await Promise.all([
                    fetchWithAuth<ScoresResponse>('/analytics/scores', { lab: selectedLabId }),
                    fetchWithAuth<TimelineResponse>('/analytics/timeline', { lab: selectedLabId }),
                    fetchWithAuth<PassRatesResponse>('/analytics/pass-rates', { lab: selectedLabId }),
                ]);

                setScores(scoresData);
                setTimeline(timelineData);
                setPassRates(passRatesData);
            } catch (err) {
                setError(err instanceof Error ? err.message : 'Failed to load analytics');
            } finally {
                setLoading(false);
            }
        };

        loadAnalytics();
    }, [selectedLabId]);

    // --- Chart Data Preparation ---

    const scoreChartData: ChartData<'bar'> = useMemo(() => {
        if (!scores) return { labels: [], datasets: [] };
        return {
            labels: scores.buckets.map((b) => b.range),
            datasets: [
                {
                    label: 'Submissions per Score Bucket',
                    data: scores.buckets.map((b) => b.count),
                    backgroundColor: 'rgba(54, 162, 235, 0.5)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1,
                },
            ],
        };
    }, [scores]);

    const timelineChartData: ChartData<'line'> = useMemo(() => {
        if (!timeline) return { labels: [], datasets: [] };
        return {
            labels: timeline.data.map((d) => d.date),
            datasets: [
                {
                    label: 'Submissions per Day',
                    data: timeline.data.map((d) => d.count),
                    borderColor: 'rgba(75, 192, 192, 1)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    tension: 0.3,
                    fill: true,
                },
            ],
        };
    }, [timeline]);

    const chartOptions: ChartOptions<'bar' | 'line'> = {
        responsive: true,
        plugins: {
            legend: {
                position: 'top',
            },
            title: {
                display: true,
                text: 'Analytics Overview',
            },
        },
    };

    // --- Handlers ---

    const handleLabChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
        setSelectedLabId(event.target.value);
    };

    // --- Render ---

    if (error) {
        return <div style={{ color: 'red', padding: '20px' }}>Error: {error}</div>;
    }

    return (
        <div style={{ padding: '20px', fontFamily: 'sans-serif' }}>
            <header style={{ marginBottom: '20px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <h1>Lab Dashboard</h1>
                <div>
                    <label htmlFor="lab-select" style={{ marginRight: '10px' }}>
                        Select Lab:
                    </label>
                    <select
                        id="lab-select"
                        value={selectedLabId}
                        onChange={handleLabChange}
                        disabled={loading || labs.length === 0}
                        style={{ padding: '5px' }}
                    >
                        {labs.length === 0 && <option value="">Loading labs...</option>}
                        {labs.map((lab) => (
                            <option key={lab.id} value={lab.id}>
                                {lab.name}
                            </option>
                        ))}
                    </select>
                </div>
            </header>

            {loading && <div>Loading analytics...</div>}

            {!loading && (
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px' }}>
                    {/* Score Buckets Bar Chart */}
                    <div style={{ border: '1px solid #ddd', padding: '15px', borderRadius: '8px' }}>
                        <h3>Score Distribution</h3>
                        <Bar data={scoreChartData} options={chartOptions as ChartOptions<'bar'>} />
                    </div>

                    {/* Timeline Line Chart */}
                    <div style={{ border: '1px solid #ddd', padding: '15px', borderRadius: '8px' }}>
                        <h3>Submission Timeline</h3>
                        <Line data={timelineChartData} options={chartOptions as ChartOptions<'line'>} />
                    </div>

                    {/* Pass Rates Table */}
                    <div style={{ gridColumn: '1 / -1', border: '1px solid #ddd', padding: '15px', borderRadius: '8px' }}>
                        <h3>Pass Rates per Task</h3>
                        {passRates && passRates.tasks.length > 0 ? (
                            <table style={{ width: '100%', borderCollapse: 'collapse', marginTop: '10px' }}>
                                <thead>
                                    <tr style={{ borderBottom: '2px solid #ddd', textAlign: 'left' }}>
                                        <th style={{ padding: '8px' }}>Task Name</th>
                                        <th style={{ padding: '8px' }}>Total Submissions</th>
                                        <th style={{ padding: '8px' }}>Pass Rate</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {passRates.tasks.map((task, index) => (
                                        <tr key={index} style={{ borderBottom: '1px solid #eee' }}>
                                            <td style={{ padding: '8px' }}>{task.taskName}</td>
                                            <td style={{ padding: '8px' }}>{task.totalSubmissions}</td>
                                            <td style={{ padding: '8px' }}>
                                                {(task.passRate * 100).toFixed(2)}%
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        ) : (
                            <p>No pass rate data available.</p>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
};

export default Dashboard;