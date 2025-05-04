import { useState } from 'react';

export const JsonClient = () => {
    const [inputJson, setInputJson] = useState<string>('');
    const [responseData, setResponseData] = useState<any>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setError(null);
        setIsLoading(true);

        try {
            const parsedJson = JSON.parse(inputJson);
            const response = await fetch(process.env.NEXT_PUBLIC_API_URL!, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(parsedJson),
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            setResponseData(data);
        } catch (err) {
            if (err instanceof Error) {
                setError(err.message);
            } else {
                setError('An unknown error occurred');
            }
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div className="container mx-auto p-4 bg-gray-50">
        <form onSubmit={handleSubmit} className="space-y-4">
    <div>
        <label htmlFor="jsonInput" className="block text-sm font-medium mb-2">
        JSON Input
    </label>
    <textarea
    id="jsonInput"
    value={inputJson}
    onChange={(e) => setInputJson(e.target.value)}
    className="w-full h-48 p-2 border rounded-md font-mono text-sm"
    placeholder='Enter JSON here...'
    required
    />
    </div>

    <button
    type="submit"
    disabled={isLoading}
    className="px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 disabled:bg-gray-400"
        >
        {isLoading ? 'Sending...' : 'Submit JSON'}
        </button>
        </form>

    {error && (
        <div className="mt-4 p-4 bg-red-100 text-red-700 rounded-md">
            Error: {error}
        </div>
    )}

    {responseData && (
        <div className="mt-4">
        <h2 className="text-lg font-medium mb-2">Response</h2>
            <pre className="p-4 bg-gray-100 rounded-md overflow-x-auto">
    <code className="text-sm">
        {JSON.stringify(responseData, null, 2)}
        </code>
        </pre>
        </div>
    )}
    </div>
);
};