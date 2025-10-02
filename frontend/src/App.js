import React from 'react';
import UploadForm from './components/UploadForm';
import PipelineList from './components/PipelineList';
import './App.css';

function App() {
  return (
    <div className="App">
      <h1>Data Engineer MVP</h1>
      <UploadForm />
      <PipelineList />
    </div>
  );
}

export default App;


