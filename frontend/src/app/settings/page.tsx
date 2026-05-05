export default function Settings() {
  return (
    <div style={{ color: '#fff', display: 'flex', flexDirection: 'column', gap: '20px' }}>
      <h1>Settings</h1>
      <div className="glass-panel" style={{ padding: '40px', textAlign: 'center', color: '#A0AAB4' }}>
        <h2>System Configuration</h2>
        <p style={{ marginTop: '16px' }}>Dashboard layout, user preferences, and notification routing configurations.</p>
      </div>
    </div>
  );
}
