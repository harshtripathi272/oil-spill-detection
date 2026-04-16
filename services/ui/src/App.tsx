import Navbar from './components/Navbar'
import Hero from './components/Hero'
import Pipeline from './components/Pipeline'
import Upload from './components/Upload'
import Results from './components/Results'
import Impact from './components/Impact'
import Footer from './components/Footer'

function App() {
  return (
    <div className="app">
      <Navbar />
      <main>
        <Hero />
        <Pipeline />
        <Upload />
        <Results />
        <Impact />
      </main>
      <Footer />
    </div>
  )
}

export default App
