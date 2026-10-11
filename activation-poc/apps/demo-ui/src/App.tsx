import { useState, useEffect } from 'react';
import { io, Socket } from 'socket.io-client';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:3000';

function App() {
  const [activations, setActivations] = useState<any[]>([]);
  const [socket, setSocket] = useState<Socket | null>(null);

  useEffect(() => {
    const newSocket = io(API_URL);
    setSocket(newSocket);
    newSocket.on('activation_update', (data) => {
      setActivations((prev) => {
        const exists = prev.findIndex(a => a._id === data._id);
        if (exists >= 0) {
          const newArray = [...prev];
          newArray[exists] = data;
          return newArray;
        }
        return [data, ...prev];
      });
    });
    return () => { newSocket.close(); };
  }, []);

  const handleContract = async (e: any) => {
    e.preventDefault();
    const formData = new FormData(e.target);
    const data = {
      customerId: formData.get('customerId'),
      planId: formData.get('planId'),
      simulateFailure: formData.get('simulateFailure')
    };

    try {
      await fetch(`${API_URL}/activations`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
      });
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div style={{ padding: '20px', fontFamily: 'sans-serif' }}>
      <h1>Demo Activaciones con Kafka</h1>
      <form onSubmit={handleContract} style={{ marginBottom: '20px', display: 'flex', gap: '10px' }}>
        <input name="customerId" placeholder="ID de Cliente" defaultValue={`C-${Math.floor(Math.random() * 10000)}`} required />
        <select name="planId">
          <option value="PLAN_DIGITAL">Plan Digital</option>
          <option value="PLAN_PREMIUM">Plan Premium</option>
        </select>
        <select name="simulateFailure">
          <option value="none">Sin fallo</option>
          <option value="billing">Fallo en Billing</option>
          <option value="provisioning">Fallo en Provisioning</option>
        </select>
        <button type="submit">Contratar Plan</button>
      </form>
      
      <div>
        <h2>Timeline de Activaciones (En vivo)</h2>
        {activations.map(act => (
          <div key={act._id} style={{ border: '1px solid #ccc', margin: '10px 0', padding: '10px' }}>
            <h3>{act._id} - Estado: {act.status}</h3>
            <p>Cliente: {act.customerId} | Plan: {act.planId}</p>
            <ul style={{ background: '#f5f5f5', padding: '10px 30px' }}>
              {act.history?.map((h: any, i: number) => (
                <li key={i} style={{ 
                  color: h.eventType.includes('Failed') ? 'red' : h.eventType.includes('Completed') ? 'green' : 'black' 
                }}>
                  {h.eventType} - {new Date(h.at).toLocaleTimeString()}
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    </div>
  );
}

export default App;
