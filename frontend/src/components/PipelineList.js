import React, { useEffect, useState } from 'react';
import axios from 'axios';

function PipelineList() {
  const [pipelines, setPipelines] = useState([]);

  const backendUrl = window.__BACKEND_URL__ || 'http://localhost:8000';

  const fetchPipelines = async () => {
    try {
      const res = await axios.get(`${backendUrl}/pipelines/`);
      setPipelines(res.data);
    } catch (err) {
      console.error(err);
    }
  };

  const handleDelete = async (id) => {
    await axios.delete(`${backendUrl}/pipelines/${id}`);
    fetchPipelines();
  };

  const handleLoad = async (id) => {
    await axios.post(`${backendUrl}/load-to-db/`, { pipeline_id: id });
    alert('Данные загружены в СУБД');
  };

  const handleHypothesis = async (id) => {
    const res = await axios.post(`${backendUrl}/hypothesis/`, { pipeline_id: id });
    alert('Гипотеза:\n' + res.data.hypothesis);
  };

  const handleUploadToHdfs = async (id) => {
    const hdfsPath = prompt('Укажите путь HDFS (например, /data/raw/sample.csv):', `/data/raw/pipeline_${id}.csv`);
    if (!hdfsPath) return;
    const res = await axios.post(`${backendUrl}/upload-to-hdfs/`, { pipeline_id: id, hdfs_path: hdfsPath });
    alert(res.data.status === 'success' ? 'Загружено в HDFS' : 'Ошибка загрузки');
  };

  const downloadDDL = (p) => {
    if (!p?.ddl_script) {
      alert('DDL отсутствует');
      return;
    }
    const blob = new Blob([p.ddl_script], { type: 'text/sql;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${p.name || 'pipeline'}.sql`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  };

  useEffect(() => {
    fetchPipelines();
    const interval = setInterval(fetchPipelines, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="pipeline-list">
      <h2>Список пайплайнов</h2>
      {pipelines.map(p => (
        <div key={p.id} className="pipeline-item">
          <p><strong>ID:</strong> {p.id} | <strong>Название:</strong> {p.name} | <strong>Target DB:</strong> {p.target_db}</p>
          <button onClick={() => handleLoad(p.id)}>Загрузить в БД</button>
          <button onClick={() => handleHypothesis(p.id)}>Сформировать гипотезу</button>
          <button onClick={() => handleUploadToHdfs(p.id)}>Загрузить в HDFS</button>
          <button onClick={() => handleDelete(p.id)}>Удалить</button>
          <details style={{ marginTop: '8px' }}>
            <summary>DDL</summary>
            <pre style={{ whiteSpace: 'pre-wrap' }}>{p.ddl_script || '—'}</pre>
            <button onClick={() => downloadDDL(p)}>Скачать DDL</button>
          </details>
          <details style={{ marginTop: '8px' }}>
            <summary>DAG (код)</summary>
            <pre style={{ whiteSpace: 'pre-wrap' }}>{p.etl_code || '—'}</pre>
          </details>
        </div>
      ))}
      {pipelines.length === 0 && <p>Пайплайнов ещё нет.</p>}
    </div>
  );
}

export default PipelineList;


